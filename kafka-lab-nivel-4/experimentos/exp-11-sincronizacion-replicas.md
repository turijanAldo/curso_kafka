# Experimento 11 - Re-sincronización de Réplicas

## Objetivo

Observar y medir el proceso de re-sincronización que ocurre cuando un broker previamente caído se recupera y sus réplicas necesitan ponerse al día con los mensajes escritos durante su ausencia.

## Hipótesis

Cuando un broker se recupera después de estar caído, sus réplicas se re-sincronizarán automáticamente con los leaders actuales. El tiempo de sincronización será proporcional a la cantidad de datos escritos mientras el broker estuvo fuera.

## Pre-requisitos

- [ ] Experimento 09 completado (broker 2 fue detenido en ese experimento)
- [ ] Topic `transacciones-rf3` con datos activos
- [ ] Java compilado

---

## Procedimiento

### Paso 1 — Verificar estado inicial (broker 2 sigue caído)

Si completaste el Experimento 09 y no recuperaste el broker, el broker 2 debería seguir caído. Verifica:

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}"
```

El ISR debería mostrar solo 2 brokers (1 y 3) para las particiones de `transacciones-rf3`.

```powershell
.\scripts\powershell\34-verificar-isr.ps1 -Topic transacciones-rf3
```

Si el broker 2 ya está corriendo, detenerlo manualmente para el experimento:
```powershell
docker stop kafka-broker-2
```

---

### Paso 2 — Producir datos mientras el broker 2 está caído

Esto simula mensajes que se escribieron durante la ausencia del broker:

```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar `
    com.nexus.kafka.nivel4.DurableProducer transacciones-rf3 5000 1
```

**CMD:**
```cmd
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar ^
    com.nexus.kafka.nivel4.DurableProducer transacciones-rf3 5000 1
```

Estos 5000 mensajes se escribirán en los brokers 1 y 3 (que están activos). El broker 2 no tendrá estos mensajes hasta que se recupere y sincronice.

---

### Paso 3 — Iniciar ISRTracker para capturar la re-sincronización

Abre una **nueva ventana** y ejecuta desde `kafka-lab-nivel-4\java\`:

```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar `
    com.nexus.kafka.nivel4.ISRTracker transacciones-rf3 --track --interval 5
```

**CMD:**
```cmd
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar ^
    com.nexus.kafka.nivel4.ISRTracker transacciones-rf3 --track --interval 5
```

El tracker tomará un snapshot del ISR cada 5 segundos. Deja esta ventana visible.

---

### Paso 4 — Recuperar el broker 2

En tu ventana principal:

```powershell
.\scripts\powershell\33-recuperar-broker.ps1 -Broker 2
```

**CMD:**
```cmd
.\scripts\cmd\33-recuperar-broker.bat 2
```

El script iniciará el broker y monitoreará automáticamente el progreso de re-sincronización.

---

### Paso 5 — Observar el ISRTracker

En la ventana del ISRTracker, observarás cómo las réplicas del broker 2 se agregan progresivamente al ISR:

**Antes:**
```
[14:30:05] Snapshot #1 | Total: 4 | Saludables: 0 (0%) | Degradadas: 4
  ⚠️  P0 | Leader: Broker1 | Replicas: [1, 2, 3] | ISR: [1, 3]
  ⚠️  P1 | Leader: Broker3 | Replicas: [2, 3, 1] | ISR: [3, 1]
```

**Durante sincronización:**
```
[14:30:45] 📈 RE-SINC: transacciones-rf3 P2 | Broker(s) [2] volvieron al ISR
[14:30:50] 📈 RE-SINC: transacciones-rf3 P0 | Broker(s) [2] volvieron al ISR
```

**Después:**
```
[14:31:00] Snapshot #12 | Total: 4 | Saludables: 4 (100%) | Degradadas: 0
  ✅ P0 | Leader: Broker1 | Replicas: [1, 2, 3] | ISR: [1, 2, 3]
  ✅ P1 | Leader: Broker3 | Replicas: [2, 3, 1] | ISR: [2, 3, 1]
```

---

### Paso 6 — Medir tiempo de re-sincronización

Anota:
- Tiempo desde que el broker 2 arrancó hasta el primer snapshot con ISR reducido
- Tiempo hasta que el primer snapshot muestra TODAS las particiones saludables

El ISRTracker también guarda datos en `experimentos/resultados/isr-historico.csv`.

---

### Paso 7 — Repetir con diferentes cantidades de datos

Para ver la relación entre datos escritos y tiempo de sincronización:

**Prueba A**: 500 mensajes escritos mientras el broker está caído
**Prueba B**: 5000 mensajes (ya hecha en el Paso 2)
**Prueba C**: 20000 mensajes (opcional)

Para cada prueba:
1. `docker stop kafka-broker-2`
2. Producir X mensajes a `transacciones-rf3`
3. Iniciar ISRTracker
4. `.\scripts\powershell\33-recuperar-broker.ps1 -Broker 2`
5. Anotar tiempo de re-sincronización

---

### Paso 8 — Verificar integridad final

```powershell
.\scripts\powershell\31-describir-replicas.ps1 -Topic transacciones-rf3
```

El estado debe mostrar todas las particiones con ISR completo [1,2,3].

También verifica el monitor de under-replicated:
```powershell
.\scripts\powershell\35-monitorear-under-replicated.ps1
```

Debería mostrar `0 under-replicated partitions`.

---

## Resultados esperados

La re-sincronización ocurre automáticamente:
1. Broker 2 arranca y se anuncia al controlador KRaft
2. El controlador le asigna sus réplicas
3. Cada réplica se conecta al leader correspondiente y empieza a copiar mensajes
4. Cuando una réplica se pone al día completamente, el controlador la agrega al ISR
5. El proceso se repite para cada partición hasta que todas están completamente sincronizadas

**Tiempo aproximado** con 5000 mensajes de ~100 bytes: 15-60 segundos en Docker local.

---

## Análisis

### ¿Qué determina la velocidad de re-sincronización?

1. **Cantidad de datos a copiar**: más mensajes = más tiempo (relación lineal)
2. **Ancho de banda de red entre brokers**: en Docker comparten la red del host
3. **Velocidad de disco**: el broker que se sincroniza escribe los mensajes en disco
4. **Carga actual del sistema**: un broker con alta carga de producción sirve también la sincronización

### ¿Por qué Kafka no agrega la réplica al ISR antes de terminar?

Por seguridad. Si una réplica se agregara al ISR antes de estar completamente sincronizada, podría convertirse en leader y causar pérdida de datos (porque le faltarían mensajes recientes). Kafka garantiza que **solo réplicas completamente al día** están en el ISR.

### El parámetro `replica.lag.time.max.ms`

Si una réplica se retrasa más de este tiempo (por defecto 30s) respecto al leader, se **elimina del ISR** aunque el broker esté activo. Esto puede pasar cuando:
- El broker está sobrecargado y no puede mantener el ritmo de replicación
- Hay problemas de red entre brokers
- El disco del broker está lento o lleno

Si reduces este parámetro a valores muy bajos (ej. 1s), réplicas se removerán del ISR más fácilmente durante picos de carga.

---

## Conclusiones

- [ ] Observé el proceso de re-sincronización automática en tiempo real
- [ ] Medí el tiempo de re-sincronización con ~5000 mensajes
- [ ] Confirmé que el ISR volvió a ser completo después de la recuperación
- [ ] Entiendo que Kafka garantiza consistencia antes de agregar una réplica al ISR
- [ ] Entiendo que el tiempo de sincronización es proporcional a los datos faltantes

---

## Mis resultados reales

### Tiempos de re-sincronización:

| Mensajes producidos durante ausencia | Tiempo de re-sincronización |
|---|---|
| 500 | |
| 5000 | |
| 20000 (opcional) | |

### Progresión del ISR observada (snapshots del ISRTracker):

| Tiempo | Particiones saludables | ISR Partition 0 |
|---|---|---|
| T+0s (inicio recuperación) | 0/4 | [1,3] |
| T+15s | | |
| T+30s | | |
| T+60s | | |
| T+_s (completado) | 4/4 | [1,2,3] |

### ¿Se rebalancearon leaders de vuelta al broker 2? [ ] Sí / [ ] No
