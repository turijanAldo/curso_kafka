# Checklist de Validación - Laboratorio Kafka Nivel 3

Marca cada item a medida que lo completas y validas.

---

## Infraestructura Docker

- [ ] `docker-compose-cluster.yml` levanta los 3 brokers sin errores
- [ ] Los 3 contenedores (`kafka-broker-1/2/3`) aparecen en `docker ps` como `Up`
- [ ] Los puertos 9092, 9093 y 9094 responden desde el host Windows
- [ ] El quorum KRaft muestra un `LeaderId` definido (no -1)
- [ ] Los 3 brokers comparten el mismo `ClusterId`

## Scripts PowerShell

- [ ] `20-iniciar-cluster.ps1` levanta el cluster y espera activamente los 3 brokers
- [ ] `21-verificar-cluster.ps1` muestra todos los topics y distribucion de particiones
- [ ] `22-describir-distribucion.ps1` genera el reporte de balance en consola y en archivo
- [ ] `23-detener-cluster.ps1` detiene el cluster y confirma los volumenes persistentes
- [ ] `24-ver-logs-brokers.ps1` muestra logs con colores diferenciados

## Scripts CMD

- [ ] `20-iniciar-cluster.bat` funciona en CMD y verifica los 3 puertos
- [ ] `21-verificar-cluster.bat` muestra descripcion de topics
- [ ] `22-describir-distribucion.bat` genera reporte en CMD
- [ ] `23-detener-cluster.bat` detiene el cluster desde CMD
- [ ] `24-ver-logs-brokers.bat` muestra logs de broker especificado

## Compilación Java

- [ ] `mvn clean package` compila sin errores
- [ ] `java\target\kafka-lab-nivel-3-1.3.0.jar` generado (~20-30 MB)
- [ ] Las clases de Niveles 1 y 2 siguen accesibles desde este JAR

## ClusterAnalyzer

- [ ] Se conecta al cluster via AdminClient
- [ ] Muestra el Cluster ID correctamente
- [ ] Identifica el controlador activo con ⭐
- [ ] Lista los 3 brokers con host y puerto
- [ ] Muestra la distribucion de particiones por topic
- [ ] Calcula estadisticas de balance correctamente

## LoadBalancedProducer

- [ ] Se conecta a los 3 brokers (bootstrap servers)
- [ ] Muestra los leaders de cada particion antes de enviar
- [ ] Envía mensajes y muestra barra de progreso
- [ ] Reporte final muestra distribucion por particion y por broker
- [ ] Con estrategia 'hash' la distribucion es balanceada (≈1/3 cada broker)

## ThroughputBenchmark

- [ ] Acepta y parsea todos los parametros (--topic, --messages, --threads, etc.)
- [ ] Ejecuta multiples threads productores
- [ ] Muestra percentiles de latencia (P50, P95, P99)
- [ ] Guarda resultados en `experimentos/resultados/metricas-throughput.txt`

## Experimento 05 - Distribución de leaders

- [ ] Topic `transacciones-6p` (6 particiones): distribución perfecta, 2 por broker
- [ ] Topic `transacciones-12p` (12 particiones): distribución perfecta, 4 por broker
- [ ] Topic `transacciones-5p` (5 particiones): distribución imperfecta observada y documentada
- [ ] Entiendo que Kafka usa round-robin para asignar leaders

## Experimento 06 - Carga balanceada

- [ ] Observé actividad simultánea en los 3 brokers con `docker stats`
- [ ] El LoadBalancedProducer confirmó ~200 mensajes por broker (de 600 total)
- [ ] Entiendo que el balanceo es automático sin código especial en el producer

## Experimento 07 - Throughput

- [ ] Ejecuté Test A (6p, 1 thread) y anoté el throughput
- [ ] Ejecuté Test B (6p, 3 threads) y anoté el throughput
- [ ] Ejecuté Test D (1p, 3 threads) y anoté el throughput
- [ ] Test B mostró mayor throughput que Test D
- [ ] Resultados guardados en `metricas-throughput.txt`

## Comprensión conceptual

- [ ] Entiendo qué es el leader de una partición y por qué solo puede haber uno
- [ ] Entiendo por qué la distribución de leaders determina la distribución de la carga
- [ ] Entiendo que más brokers + más particiones = más throughput horizontal
- [ ] Entiendo que el controlador KRaft gestiona el cluster sin ZooKeeper
- [ ] Entiendo por qué usar múltiplos del número de brokers para las particiones
- [ ] Entiendo los percentiles de latencia y su importancia para SLAs

---

## Notas personales

*(Observaciones, preguntas o hallazgos durante el laboratorio)*

```

```

---

**Nivel 3 completado el:** _______________

**Tiempo total invertido:** _______________

**Throughput con 6p/3 threads vs 1p/3 threads:** ___x de mejora

**Listo para el Nivel 4 (réplicas y alta disponibilidad):** [ ] Sí / [ ] Necesito repasar: _______________
