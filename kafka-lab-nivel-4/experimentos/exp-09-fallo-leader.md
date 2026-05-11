# Experimento 09 - Fallo de Leader y Failover Automático

## Objetivo

Demostrar que cuando un broker que es leader de varias particiones falla, Kafka elige automáticamente nuevos leaders de entre los followers in-sync, permitiendo que el sistema continúe operando sin intervención manual.

## Hipótesis

Si detengo el broker 2 abruptamente mientras hay tráfico activo, las particiones donde el broker 2 es leader experimentarán failover automático a otros brokers dentro de aproximadamente 2-5 segundos. No se perderán mensajes con acks=all.

## Pre-requisitos

- [ ] Topics replicados creados (Experimento 08 completado)
- [ ] `transacciones-rf3` con algunas particiones lideradas por el broker 2
- [ ] Java compilado
- [ ] Necesitas 3 ventanas de terminal simultáneamente

---

## Configuración: 3 ventanas de terminal

```
┌──────────────────────────┬──────────────────────────┐
│   Ventana 1              │   Ventana 2              │
│   FailoverMonitor        │   DurableProducer        │
│   (observa en tiempo     │   (produce mensajes      │
│    real los eventos)     │    durante el fallo)     │
├──────────────────────────┴──────────────────────────┤
│   Ventana 3                                         │
│   Script de fallo (32-simular-fallo-broker.ps1)     │
└─────────────────────────────────────────────────────┘
```

---

## Procedimiento

### Paso 1 — Capturar estado inicial

```powershell
.\scripts\powershell\31-describir-replicas.ps1
```

Busca qué particiones tienen `Leader: 2`. Anótalas — esas experimentarán failover.

---

### Paso 2 — Ventana 1: iniciar FailoverMonitor

Abre una **nueva ventana** de PowerShell y ejecuta desde `kafka-lab-nivel-4\java\`:

```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar `
    com.nexus.kafka.nivel4.FailoverMonitor transacciones-rf3
```

**CMD:**
```cmd
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar ^
    com.nexus.kafka.nivel4.FailoverMonitor transacciones-rf3
```

Deja esta ventana visible. Verás eventos en tiempo real.

---

### Paso 3 — Ventana 2: iniciar producer continuo

Abre **otra ventana nueva** y ejecuta desde `kafka-lab-nivel-4\java\`:

```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar `
    com.nexus.kafka.nivel4.DurableProducer transacciones-rf3 continuous all
```

Verás mensajes enviándose continuamente. Esto simula tráfico activo durante el fallo.

---

### Paso 4 — Ventana 3: simular el fallo

En tu ventana principal, desde `kafka-lab-nivel-4\`:

```powershell
.\scripts\powershell\32-simular-fallo-broker.ps1 -Broker 2
```

**CMD:**
```cmd
.\scripts\cmd\32-simular-fallo-broker.bat 2
```

Escribe `SI` cuando pida confirmación.

---

### Paso 5 — Observar el failover en tiempo real

**En la Ventana 1 (FailoverMonitor)** deberías ver algo como:

```
[14:24:15.789] ⚠️  BROKER CAIDO DETECTADO
               • Broker 2 ya no responde
               • 2 particiones tenian su leader en Broker 2

[14:24:16.234] 🔄 FAILOVER DETECTADO
               • Topic: transacciones-rf3, Partition: 1
               • Leader: Broker 2 → Broker 3
               • Tiempo de failover: 445 ms

[14:24:16.567] 📉 REPLICAS REMOVIDAS DEL ISR
               • Topic: transacciones-rf3, Partition: 1
               • ISR antes: [2, 3, 1]
               • ISR ahora: [3, 1]
```

**En la Ventana 2 (DurableProducer)** puedes ver algunos errores temporales mientras Kafka completa el failover, seguidos de reintentos automáticos.

---

### Paso 6 — Verificar estado post-fallo (30 segundos después)

```powershell
.\scripts\powershell\31-describir-replicas.ps1
```

Observa:
- Las particiones que tenían Leader=2 ahora tienen Leader=1 o Leader=3
- El ISR de esas particiones muestra solo 2 brokers (sin el broker 2 caído)
- Las demás particiones siguen con sus leaders originales

```powershell
.\scripts\powershell\34-verificar-isr.ps1
```

Verás advertencias para las particiones con ISR incompleto.

---

### Paso 7 — Detener el producer y verificar integridad de datos

Detén el producer con `Ctrl+C` en la Ventana 2.

Cuenta los mensajes reales en el topic:

```powershell
docker exec kafka-broker-1 /opt/kafka/bin/kafka-run-class.sh `
    kafka.tools.GetOffsetShell `
    --bootstrap-server localhost:9092 `
    --topic transacciones-rf3 `
    --time -1
```

La suma de offsets por partición representa el total de mensajes confirmados.

---

### Paso 8 — Verificar el monitor de under-replicated

```powershell
.\scripts\powershell\35-monitorear-under-replicated.ps1
```

Con el broker 2 caído, verás que las particiones que tenían réplicas en ese broker están under-replicated. El script sugerirá ejecutar el script de recuperación.

---

## Resultados esperados

Deberías observar:

1. **Tiempo de detección del fallo**: 1-3 segundos desde que el broker se detiene hasta que Kafka lo detecta
2. **Tiempo de failover**: 200-800 ms desde la detección hasta que el nuevo leader está sirviendo tráfico
3. **Continuidad del sistema**: el producer continúa operando (con posibles reintentos breves)
4. **ISR reducido**: particiones que tenían réplicas en broker 2 muestran ISR=[1,3] en lugar de ISR=[1,2,3]
5. **Sin pérdida de datos**: con acks=all, todos los mensajes confirmados deben estar en el topic

---

## Análisis

**¿Por qué el failover es rápido?**

Kafka mantiene el ISR actualizado en tiempo real. Cuando el broker falla, el controlador KRaft ya sabe qué réplicas estaban sincronizadas. La elección del nuevo leader es inmediata: el controlador simplemente designa la primera réplica del ISR como nuevo leader.

**¿Qué habría pasado con RF=1?**

Con `transacciones-rf1`, las particiones cuyo único broker era el 2 habrían quedado completamente **inaccesibles**. No hay followers que puedan tomar el rol de leader. Eso es la diferencia crítica entre RF=1 y RF>1.

**¿Por qué el producer tiene errores temporales con acks=all?**

Porque el producer intenta enviar al broker 2 (que acaba de caer), recibe un error de conexión, y Kafka automáticamente reintenta. Una vez que el producer actualiza sus metadatos y conoce el nuevo leader, los reintentos tienen éxito.

---

## Conclusiones

- [ ] Observé el failover automático en tiempo real con el FailoverMonitor
- [ ] Medí el tiempo de failover: _____ ms (desde detección hasta nuevo leader)
- [ ] El sistema continuó operando a pesar de perder 1 de 3 brokers
- [ ] Con acks=all no se perdieron mensajes durante el failover
- [ ] Los topics con RF=1 habrían quedado inaccesibles

**Para recuperar el broker 2:** ejecuta el Experimento 11 o el script 33.

---

## Mis resultados reales

### Estado ANTES del fallo:
| Topic | Partition | Leader | Replicas | ISR |
|---|---|---|---|---|
| transacciones-rf3 | | | | |
| transacciones-rf3 | | | | |
| transacciones-rf3 | | | | |
| transacciones-rf3 | | | | |

### Particiones afectadas (Leader=2 antes del fallo):
_Anotar qué particiones tenían leader en Broker 2:_

### Tiempos de failover observados:
| Partición | Tiempo de failover (ms) |
|---|---|
| | |
| | |

### Estado DURANTE el fallo (ISR reducido):
| Topic | Partition | Nuevo Leader | ISR |
|---|---|---|---|
| | | | |

### ¿Se perdieron mensajes? [ ] No / [ ] Sí, cuántos: ___
