# Checklist de Validación - Laboratorio Kafka Nivel 2

Marca cada item a medida que lo completas y validas.

---

## Topics y configuración

- [ ] Topic `transacciones-1p` creado con exactamente 1 partición
- [ ] Topic `transacciones-4p` creado con exactamente 4 particiones
- [ ] Topic `transacciones-8p` creado con exactamente 8 particiones
- [ ] El script `11-describir-particiones` muestra correctamente Leader, Replicas e ISR

## Compilación Java

- [ ] `mvn clean package` ejecuta sin errores
- [ ] Archivo `java/target/kafka-lab-nivel-2-1.0.0.jar` generado
- [ ] `JAVA_HOME` configurado y `$env:JAVA_HOME\bin\java` funciona

## KeyedProducer

- [ ] Muestra el hash murmur2 de la clave antes de enviar
- [ ] Muestra la partición teórica calculada (`hash % numParticiones`)
- [ ] La partición real coincide con la partición teórica
- [ ] Mensajes con la misma clave van siempre a la misma partición (confirmado con 5+ envíos)

## InstrumentedConsumer

- [ ] Muestra `🎯 ASIGNADO A: Partitions [X]` al iniciar
- [ ] Muestra `⚠️ REVOCADAS: Partitions [X]` cuando ocurre un rebalanceo
- [ ] Muestra `🎯 ASIGNADO A: Partitions [X, Y]` tras el rebalanceo (particiones nuevas)
- [ ] El resumen al cerrar (Ctrl+C) muestra correctamente cuántos mensajes procesó por partición

## BatchProducer

- [ ] Envía 1000 mensajes correctamente a `transacciones-4p`
- [ ] Muestra barra de progreso durante el envío
- [ ] Muestra throughput (mensajes/segundo) al finalizar
- [ ] Muestra distribución de mensajes por partición al finalizar

## PartitionAnalyzer

- [ ] Muestra tabla con earliest offset, latest offset y mensajes por partición
- [ ] El total de mensajes coincide con lo enviado por BatchProducer
- [ ] Indica si la distribución es balanceada o desbalanceada

## Experimento 02 - Distribución por clave

- [ ] `user-123` siempre fue a la misma partición (verificado con 10+ envíos)
- [ ] `user-456` siempre fue a la misma partición (puede ser igual o diferente a `user-123`)
- [ ] `user-789` siempre fue a la misma partición
- [ ] Entiendo por qué el hash determina la partición y por qué es determinista

## Experimento 03 - Paralelismo de consumers

- [ ] Con 1 consumer → asignado a particiones [0, 1, 2, 3]
- [ ] Al agregar el 2do consumer → rebalanceo visible con REVOCADAS y nuevo ASIGNADO
- [ ] Con 4 consumers → cada uno tiene exactamente 1 partición
- [ ] Con 5 consumers → el 5to muestra `ASIGNADO A: Partitions []` (idle)
- [ ] Al cerrar un consumer → los demás reciben sus particiones via rebalanceo

## Experimento 04 - Throughput

- [ ] Medí el tiempo de consumo con 1 consumer en `transacciones-1p`
- [ ] Medí el tiempo de consumo con 4 consumers en `transacciones-4p`
- [ ] El procesamiento paralelo fue notablemente más rápido
- [ ] Entiendo por qué más particiones solas no mejoran nada sin más consumers

## Comprensión conceptual

- [ ] Entiendo qué determina la partición de un mensaje (con y sin clave)
- [ ] Entiendo la regla "1 partición = máximo 1 consumer por grupo a la vez"
- [ ] Entiendo qué es el rebalanceo y cuándo ocurre
- [ ] Entiendo por qué las claves son importantes para mantener orden por entidad
- [ ] Entiendo el trade-off entre más particiones y complejidad operativa
- [ ] Entiendo qué significa ISR (In-Sync Replicas)

---

## Notas personales

*(Observaciones, preguntas o hallazgos durante el laboratorio)*

```

```

---

**Nivel 2 completado el:** _______________

**Tiempo total invertido:** _______________

**Listo para el Nivel 3 (multi-broker):** [ ] Sí / [ ] Necesito repasar: _______________
