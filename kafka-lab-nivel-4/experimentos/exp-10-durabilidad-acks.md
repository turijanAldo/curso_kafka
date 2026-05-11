# Experimento 10 - Durabilidad: Comparación de Niveles de ACKs

## Objetivo

Medir empíricamente cómo diferentes configuraciones de acknowledgment (`acks=0`, `acks=1`, `acks=all`) afectan el throughput, la latencia y las garantías de durabilidad bajo condiciones normales y bajo fallo de brokers.

## Hipótesis

`acks=all` proporciona las mejores garantías de durabilidad a costa de menor throughput. `acks=0` maximiza throughput pero no garantiza durabilidad. `acks=1` es un balance intermedio. La diferencia se hace crítica cuando hay fallos de brokers.

## Pre-requisitos

- [ ] Topic `critical-data` creado (RF=3, min.insync.replicas=2)
- [ ] Cluster de 3 brokers operacional
- [ ] Java compilado

---

## Procedimiento: Tests de rendimiento

Todos los tests se ejecutan desde `kafka-lab-nivel-4\java\`.

### Test 1 — acks=0 (sin confirmación, máximo throughput)

```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar `
    com.nexus.kafka.nivel4.DurableProducer critical-data 5000 0
```

**CMD:**
```cmd
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar ^
    com.nexus.kafka.nivel4.DurableProducer critical-data 5000 0
```

Anota el throughput (msg/s) y latencia P95 de la salida del programa.

---

### Test 2 — acks=1 (solo leader confirma)

```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar `
    com.nexus.kafka.nivel4.DurableProducer critical-data 5000 1
```

Compara el throughput con el Test 1. Deberías ver una reducción.

---

### Test 3 — acks=all (leader + ISR confirman, máxima durabilidad)

```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar `
    com.nexus.kafka.nivel4.DurableProducer critical-data 5000 all
```

Este test usa el topic `critical-data` que tiene `min.insync.replicas=2`. Eso significa que el producer espera confirmación del leader **y** al menos un follower más antes de considerar la escritura exitosa.

---

## Procedimiento: Tests de durabilidad bajo fallo

Para ver empíricamente la diferencia de durabilidad, necesitas 2 ventanas.

### Test 4 — acks=0 bajo fallo de broker

**Ventana 1:** Inicia el producer en modo continuo con acks=0:
```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar `
    com.nexus.kafka.nivel4.DurableProducer critical-data continuous 0
```

**Ventana 2:** Espera 5 segundos y simula un fallo:
```powershell
.\scripts\powershell\32-simular-fallo-broker.ps1 -Broker 2
```

Observa: con `acks=0`, el producer **no recibe errores** aunque el broker caiga. Los mensajes enviados en el momento del fallo pueden perderse silenciosamente. Cuando el producer actualiza sus metadatos y descubre el nuevo leader, continúa enviando sin saber cuántos mensajes se perdieron.

Detén el producer con Ctrl+C. Recupera el broker 2:
```powershell
.\scripts\powershell\33-recuperar-broker.ps1 -Broker 2
```

---

### Test 5 — acks=all bajo fallo de broker

Asegúrate de que el broker 2 está recuperado. Luego:

**Ventana 1:** Producer continuo con acks=all:
```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar `
    com.nexus.kafka.nivel4.DurableProducer critical-data continuous all
```

**Ventana 2:** Simula fallo del broker 2:
```powershell
.\scripts\powershell\32-simular-fallo-broker.ps1 -Broker 2
```

Observa: con `acks=all`, el producer **sí recibe errores** durante el failover (la ventana donde el broker acaba de caer y el nuevo leader aún no está establecido). El producer reintenta automáticamente. Después del failover, los reintentos tienen éxito porque el ISR aún tiene 2 brokers (≥ min.insync.replicas=2).

**Los mensajes confirmados antes del fallo están garantizados** en al menos 2 replicas.

---

## Tabla comparativa a completar

```
╔════════════════════════════════════════════════════════╗
║        COMPARACIÓN DE NIVELES DE ACKS                  ║
╚════════════════════════════════════════════════════════╝
```

| Configuración | Throughput (msg/s) | Latencia P50 (ms) | Latencia P95 (ms) | Mensajes perdidos bajo fallo |
|---|---|---|---|---|
| acks=0  | | | | Posibles (silenciosos) |
| acks=1  | | | | 0 (detectados, reintentos) |
| acks=all | | | | 0 (garantizados) |

Guarda tus resultados en `experimentos/resultados/comparacion-acks.txt`.

---

## Análisis

### ¿Por qué acks=all es más lento?

Con `acks=all`, el producer espera que el mensaje se escriba en el disco del leader **Y** que el leader propague el mensaje a todos los ISR followers **Y** que cada follower confirme que lo escribió. Este round-trip adicional agrega latencia.

```
acks=0:   Producer → [FIRE AND FORGET] → continúa
acks=1:   Producer → Leader → [ACK] → continúa
acks=all: Producer → Leader → Follower1 + Follower2 → [ACK] → Leader → [ACK] → continúa
```

### ¿Cuándo usar cada nivel?

| Caso de uso | acks recomendado | Justificación |
|---|---|---|
| Transacciones financieras | `all` | Pérdida inaceptable |
| Eventos de auditoría/compliance | `all` | Requerimiento legal |
| Logs de aplicación importantes | `1` | Balance razonable |
| Métricas de monitoreo | `1` o `0` | Alta frecuencia, pérdida tolerable |
| Telemetría IoT de alta velocidad | `0` | Throughput crítico |

### min.insync.replicas y su interacción con acks=all

`min.insync.replicas=2` en el topic `critical-data` significa:
- Con acks=all y 3 brokers: necesita 2 confirmaciones → puede tolerar 1 broker caído
- Con acks=all y solo 1 broker en ISR: **el producer recibirá NotEnoughReplicasException**

Esto protege contra la situación donde el leader confirma una escritura pero inmediatamente después falla, antes de que ningún follower la replicara.

---

## Conclusiones

- [ ] Medí el throughput con los 3 niveles de acks
- [ ] acks=0 fue el más rápido pero sin garantías
- [ ] acks=all fue el más lento pero con máxima durabilidad
- [ ] Observé cómo cada nivel maneja el fallo de un broker
- [ ] Entiendo el trade-off: throughput vs durabilidad

### Mi observación personal sobre el trade-off:

_Escribe aquí qué nivel de acks usarías para tu caso de uso específico y por qué:_

```

```

### Mejora de throughput de acks=0 vs acks=all observada: ___x

### ¿Qué nivel de acks usarías para datos de transacciones? ___
