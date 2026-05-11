# Experimento 08 - Réplicas Básicas e ISR

## Objetivo

Entender cómo se configuran y distribuyen las réplicas de particiones en un clúster Kafka y qué significa el concepto de In-Sync Replicas (ISR). Verificar que con RF=3 cada partición existe en los 3 brokers simultáneamente.

## Hipótesis

Con un topic de 4 particiones y RF=3 en un clúster de 3 brokers, cada partición existirá como 3 copias completas (una en cada broker), y en estado saludable todas las réplicas estarán in-sync con el leader.

## Conceptos previos necesarios

- Qué es una partición y qué es un leader de partición (Niveles 2 y 3)
- Cómo múltiples brokers comparten la carga de trabajo (Nivel 3)
- **Nuevo**: Una **réplica** es una copia completa de una partición en otro broker
- **Nuevo**: El **ISR** (In-Sync Replicas) es el conjunto de réplicas que están completamente al día con el leader
- **Diferencia clave**: `Replicas` = lista de brokers que *deberían* tener la copia. `Isr` = lista de brokers que *actualmente tienen* la copia al día.

## Pre-requisitos

- [ ] Clúster de 3 brokers iniciado
- [ ] Scripts de Nivel 4 disponibles
- [ ] Java compilado: `cd java && mvn clean package`
- [ ] `JAVA_HOME` configurado

---

## Procedimiento

### Paso 1 — Iniciar y verificar el clúster

```powershell
# Desde kafka-lab-nivel-3
.\scripts\powershell\20-iniciar-cluster.ps1
.\scripts\powershell\21-verificar-cluster.ps1
```

Confirma que los 3 brokers aparecen con estado `Up`.

---

### Paso 2 — Crear los topics replicados

```powershell
# Desde kafka-lab-nivel-4
.\scripts\powershell\30-crear-topics-replicados.ps1
```

**CMD:**
```cmd
.\scripts\cmd\30-crear-topics-replicados.bat
```

Observa la salida de `--describe` para cada topic. Presta atención a las columnas:
- `Leader`: el broker que sirve lecturas y escrituras
- `Replicas`: todos los brokers que tienen (o deberían tener) una copia
- `Isr`: brokers actualmente sincronizados

---

### Paso 3 — Analizar el topic transacciones-rf3 con el script

```powershell
.\scripts\powershell\31-describir-replicas.ps1 -Topic transacciones-rf3
```

Documenta el estado de cada partición en la tabla de resultados al final de este archivo.

---

### Paso 4 — Analizar con ReplicaAnalyzer (Java)

Desde `kafka-lab-nivel-4\java\`:

```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar `
    com.nexus.kafka.nivel4.ReplicaAnalyzer transacciones-rf3
```

**CMD:**
```cmd
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar ^
    com.nexus.kafka.nivel4.ReplicaAnalyzer transacciones-rf3
```

Compara la salida con el script PowerShell. Deben mostrar la misma información.

---

### Paso 5 — Producir mensajes y verificar que el ISR se mantiene

```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar `
    com.nexus.kafka.nivel4.DurableProducer transacciones-rf3 500 all
```

Mientras se envían mensajes, en otra ventana verifica que el ISR no cambia:

```powershell
.\scripts\powershell\34-verificar-isr.ps1 -Topic transacciones-rf3
```

El ISR debería permanecer completo (todos los brokers in-sync) incluso durante la producción activa.

---

### Paso 6 — Comparar topics con diferentes RF

Analiza los 4 topics para ver la diferencia visual entre RF=1, RF=2, RF=3:

```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar `
    com.nexus.kafka.nivel4.ReplicaAnalyzer
```

(Sin argumentos analiza todos los topics)

**Observa:**
- `transacciones-rf1`: Replicas=[X], ISR=[X] → solo 1 copia
- `transacciones-rf2`: Replicas=[X,Y], ISR=[X,Y] → 2 copias
- `transacciones-rf3`: Replicas=[X,Y,Z], ISR=[X,Y,Z] → 3 copias (todas in-sync)
- `critical-data`: igual que rf3 pero con restricción adicional de min.insync.replicas=2

---

### Paso 7 — Ver el impacto en espacio de almacenamiento

Con RF=3, cada mensaje ocupa 3x más espacio total en el clúster (aunque cada broker almacena solo su copia). En Docker puedes ver los volúmenes:

```powershell
docker system df -v | Select-String "kafka-data"
```

---

## Resultados esperados

Para `transacciones-rf3`, cada partición debería mostrar:

```
Topic: transacciones-rf3
  ✅ Partition 0
     Leader  : Broker 1
     Replicas: [1, 2, 3]
     ISR     : [1, 2, 3]
     Estado  : COMPLETAMENTE REPLICADO
  ✅ Partition 1
     Leader  : Broker 2
     Replicas: [2, 3, 1]
     ISR     : [2, 3, 1]
     Estado  : COMPLETAMENTE REPLICADO
  ...
```

En un clúster saludable sin carga extrema: **ISR = Replicas** para todas las particiones.

---

## Análisis

**¿Por qué es importante el ISR?**

El ISR es crítico para la tolerancia a fallos porque **Kafka solo elige el nuevo leader de entre las réplicas IN-SYNC**. Si una réplica no está en el ISR cuando el leader falla, no puede convertirse en el nuevo leader porque podría no tener los mensajes más recientes.

**Trade-off de almacenamiento:**

| RF | Copias | Espacio | Tolerancia a fallos |
|---|---|---|---|
| 1  | 1 | 1x  | Ninguna (pérdida si falla el broker) |
| 2  | 2 | 2x  | 1 broker puede fallar |
| 3  | 3 | 3x  | 2 brokers pueden fallar simultáneamente |

**replica.lag.time.max.ms**: Si una réplica no se sincroniza con el leader por más de este tiempo (por defecto 30 segundos), se remueve del ISR. Cuando el broker vuelve o se pone al día, se re-agrega automáticamente.

---

## Conclusiones

- [ ] Confirmé que con RF=3 cada partición existe en los 3 brokers
- [ ] Entiendo la diferencia entre `Replicas` (configurado) e `ISR` (actualmente in-sync)
- [ ] Entiendo que el ISR determina qué réplicas pueden convertirse en leader
- [ ] Entiendo el trade-off: más réplicas = más durabilidad + más espacio de disco

---

## Mis resultados reales

### `transacciones-rf3`:
| Partition | Leader | Replicas | ISR | Estado |
|---|---|---|---|---|
| 0 | | | | |
| 1 | | | | |
| 2 | | | | |
| 3 | | | | |

### `critical-data`:
| Partition | Leader | Replicas | ISR | min.insync.replicas |
|---|---|---|---|---|
| 0 | | | | 2 |
| 1 | | | | 2 |

### ¿Todos los topics mostraron ISR completo en estado saludable? [ ] Sí / [ ] No
