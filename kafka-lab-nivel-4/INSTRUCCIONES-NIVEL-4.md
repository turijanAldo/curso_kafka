# Instrucciones Detalladas - Laboratorio Kafka Nivel 4

## Introducción

En este nivel agregas la pieza que hace a Kafka verdaderamente robusto: **la replicación de particiones**. Hasta ahora usaste `replication-factor=1`, lo que significa que cada partición existe en un solo broker. Si ese broker falla, pierdes los datos y la partición queda inaccesible.

En este nivel configurarás réplicas múltiples, simularás fallos de brokers y observarás cómo Kafka continúa operando automáticamente sin pérdida de datos ni intervención manual.

**Lo que aprenderás:**
- Qué son las réplicas y cómo se distribuyen entre brokers
- Qué es el ISR (In-Sync Replicas) y por qué es el concepto central de la durabilidad en Kafka
- Cómo Kafka elige automáticamente nuevos leaders cuando un broker falla (failover)
- Cómo los niveles de acks (0, 1, all) afectan throughput vs durabilidad
- Cómo un broker se re-sincroniza automáticamente después de recuperarse de un fallo

**Nota importante:** Este nivel usa el mismo clúster de 3 brokers del Nivel 3. No necesitas un nuevo docker-compose.

---

## Sección 1: Preparación del entorno

### 1.1 Verificar que el Nivel 3 está funcional

```powershell
# Desde kafka-lab-nivel-3
.\scripts\powershell\20-iniciar-cluster.ps1
.\scripts\powershell\21-verificar-cluster.ps1
```

Confirma que los 3 brokers están activos y el quorum KRaft tiene un `LeaderId` definido.

### 1.2 Verificar JAVA_HOME

```powershell
echo $env:JAVA_HOME
# Si está vacío:
$env:JAVA_HOME = "C:\Program Files\Java\jdk-17"
```

---

## Sección 2: Compilación del código Java del Nivel 4

Desde el directorio `kafka-lab-nivel-4\java\`:

```powershell
mvn clean package
```

**JAR generado:** `java\target\kafka-lab-nivel-4-1.4.0.jar`

### Clases disponibles en el JAR:

```
com.nexus.kafka.nivel1.SimpleProducer        (del Nivel 1)
com.nexus.kafka.nivel1.SimpleConsumer        (del Nivel 1)
com.nexus.kafka.nivel2.KeyedProducer         (del Nivel 2)
com.nexus.kafka.nivel2.InstrumentedConsumer  (del Nivel 2)
com.nexus.kafka.nivel2.BatchProducer         (del Nivel 2)
com.nexus.kafka.nivel2.PartitionAnalyzer     (del Nivel 2)
com.nexus.kafka.nivel3.ClusterAnalyzer       (del Nivel 3)
com.nexus.kafka.nivel3.LoadBalancedProducer  (del Nivel 3)
com.nexus.kafka.nivel3.ThroughputBenchmark   (del Nivel 3)
com.nexus.kafka.nivel4.ReplicaAnalyzer       ← NUEVO
com.nexus.kafka.nivel4.DurableProducer       ← NUEVO
com.nexus.kafka.nivel4.FailoverMonitor       ← NUEVO
com.nexus.kafka.nivel4.ISRTracker            ← NUEVO
```

### Formato del comando Java (desde `kafka-lab-nivel-4\java\`):

```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar com.nexus.kafka.nivel4.NombreClase [args]
```

**CMD:**
```cmd
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar com.nexus.kafka.nivel4.NombreClase [args]
```

---

## Sección 3: Crear topics replicados

```powershell
# Desde kafka-lab-nivel-4
.\scripts\powershell\30-crear-topics-replicados.ps1
```

Esto crea 4 topics:
- `transacciones-rf1`: 4 particiones, RF=1 (línea base)
- `transacciones-rf2`: 4 particiones, RF=2 (tolera 1 fallo)
- `transacciones-rf3`: 4 particiones, RF=3 (tolera 2 fallos)
- `critical-data`: 2 particiones, RF=3, min.insync.replicas=2

Luego analiza su estado inicial:

```powershell
.\scripts\powershell\31-describir-replicas.ps1
```

---

## Sección 4: Experimento 08 - Réplicas básicas

Sigue `experimentos\exp-08-replicas-basicas.md` paso a paso.

**Puntos clave:**
- Verifica que `transacciones-rf3` tiene 3 réplicas por partición, todas in-sync
- Usa el `ReplicaAnalyzer` para ver la distribución programáticamente
- Entiende la diferencia entre `Replicas` (configurado) e `ISR` (actualmente sincronizado)

```powershell
& "$env:JAVA_HOME\bin\java" -cp java\target\kafka-lab-nivel-4-1.4.0.jar `
    com.nexus.kafka.nivel4.ReplicaAnalyzer transacciones-rf3
```

---

## Sección 5: Experimento 09 - Fallo de leader

Sigue `experimentos\exp-09-fallo-leader.md`. Este es el experimento más importante del nivel.

**Configuración necesaria:** 3 ventanas de terminal simultáneas:

| Ventana | Qué ejecutar |
|---|---|
| 1 | `FailoverMonitor` (observa eventos en tiempo real) |
| 2 | `DurableProducer ... continuous all` (tráfico activo) |
| 3 | `32-simular-fallo-broker.ps1 -Broker 2` (simula el fallo) |

**Lo que debes observar:**
- El FailoverMonitor reportando el fallo y el failover en tiempo real
- El tiempo de failover (desde detección hasta nuevo leader)
- El producer recuperándose automáticamente después del failover
- El ISR reducido en las particiones del broker caído

---

## Sección 6: Experimento 10 - Durabilidad de acks

Sigue `experimentos\exp-10-durabilidad-acks.md`.

Ejecuta el `DurableProducer` con cada nivel de acks y registra los resultados en la tabla del experimento. Los resultados también se guardan automáticamente en `experimentos\resultados\comparacion-acks.txt`.

**Comando clave:**
```powershell
# Cambiar el último argumento: 0, 1, o all
& "$env:JAVA_HOME\bin\java" -cp java\target\kafka-lab-nivel-4-1.4.0.jar `
    com.nexus.kafka.nivel4.DurableProducer critical-data 5000 all
```

---

## Sección 7: Experimento 11 - Sincronización de réplicas

Sigue `experimentos\exp-11-sincronizacion-replicas.md`.

Antes de recuperar el broker, inicia el `ISRTracker` en modo tracking para capturar todo el proceso:

```powershell
& "$env:JAVA_HOME\bin\java" -cp java\target\kafka-lab-nivel-4-1.4.0.jar `
    com.nexus.kafka.nivel4.ISRTracker transacciones-rf3 --track --interval 5
```

Luego recupera el broker:

```powershell
.\scripts\powershell\33-recuperar-broker.ps1 -Broker 2
```

Observa cómo las particiones van volviendo a ISR completo progresivamente.

---

## Sección 8: Exploración adicional

### Simular fallo de 2 brokers simultáneamente

Con RF=3 puedes tolerar hasta 2 brokers fallando:

```powershell
docker stop kafka-broker-2
docker stop kafka-broker-3
# El cluster sigue operando con solo el broker 1
# Todos los leaders ahora están en el broker 1
.\scripts\powershell\34-verificar-isr.ps1
docker start kafka-broker-2
docker start kafka-broker-3
```

### Monitoreo continuo de ISR

```powershell
.\scripts\powershell\34-verificar-isr.ps1 -Watch
```

### Monitor de under-replicated partitions

```powershell
.\scripts\powershell\35-monitorear-under-replicated.ps1
```

---

## Sección 9: Decisiones de diseño para producción

Ahora tienes el conocimiento para tomar decisiones informadas:

| Caso de uso | RF | min.insync | acks | Justificación |
|---|---|---|---|---|
| Transacciones financieras | 3 | 2 | all | Pérdida inaceptable |
| Eventos de auditoría | 3 | 2 | all | Requerimiento legal |
| Logs de aplicación | 2 | 1 | 1 | Balance razonable |
| Métricas de monitoreo | 2 | 1 | 1 | Alta frecuencia |
| Telemetría IoT | 2 | 1 | 0 | Throughput crítico |

**Regla práctica**: `min.insync.replicas = RF - 1`. Con RF=3 usa `min.insync.replicas=2`. Esto garantiza que puedes tolerar 1 broker fallando sin hacer el topic read-only para producers con `acks=all`.

---

## Sección 10: Limpieza

### Detener el clúster (conservar datos)

```powershell
# Desde kafka-lab-nivel-3
.\scripts\powershell\23-detener-cluster.ps1
```

### Eliminar todo (incluyendo datos y topics)

```powershell
# Desde kafka-lab-nivel-3\docker
docker-compose -f docker-compose-cluster.yml down -v
```

---

## Sección 11: Próximos pasos

¡Felicitaciones! Has completado los 4 niveles del laboratorio Kafka. Ahora tienes conocimiento profundo de:

- **Nivel 1**: Kafka standalone, KRaft, primer mensaje
- **Nivel 2**: Particiones, claves, consumer groups, rebalancing
- **Nivel 3**: Clúster multi-broker, distribución de leaders, throughput horizontal
- **Nivel 4**: Réplicas, ISR, failover automático, durabilidad vs rendimiento

**Posibles direcciones para continuar:**

1. **Kafka Streams**: procesamiento de datos en tiempo real directamente en Kafka
2. **Kafka Connect**: integración con bases de datos, S3, Elasticsearch, etc.
3. **Schema Registry**: gestión de evolución de esquemas (Avro, Protobuf)
4. **Seguridad**: SASL/SSL, ACLs, encriptación en reposo y en tránsito
5. **Operaciones avanzadas**: tuning de JVM, compaction, retention policies, mirror-maker
6. **Kubernetes**: desplegar Kafka en contenedores orquestados con operadores como Strimzi
