# Checklist de Validación - Laboratorio Kafka Nivel 4

Marca cada item a medida que lo completas y validas.

---

## Infraestructura

- [ ] El clúster de 3 brokers del Nivel 3 arranca sin errores
- [ ] Los 3 brokers están activos durante todos los experimentos
- [ ] Docker Desktop tiene al menos 6 GB de RAM asignados

## Scripts PowerShell

- [ ] `30-crear-topics-replicados.ps1` crea los 4 topics correctamente
- [ ] `31-describir-replicas.ps1` muestra el estado de ISR por partición
- [ ] `32-simular-fallo-broker.ps1` detiene el broker y monitorea el failover
- [ ] `33-recuperar-broker.ps1` reinicia el broker y rastrea la re-sincronización
- [ ] `34-verificar-isr.ps1` clasifica particiones en saludables/advertencia/críticas
- [ ] `35-monitorear-under-replicated.ps1` detecta y reporta under-replicated partitions

## Scripts CMD

- [ ] `30-crear-topics-replicados.bat` funciona desde CMD
- [ ] `31-describir-replicas.bat` muestra describe de topics
- [ ] `32-simular-fallo-broker.bat` detiene el broker indicado
- [ ] `33-recuperar-broker.bat` recupera el broker
- [ ] `34-verificar-isr.bat` muestra estado del ISR
- [ ] `35-monitorear-under-replicated.bat` detecta under-replicated

## Compilación Java

- [ ] `mvn clean package` compila sin errores
- [ ] `java\target\kafka-lab-nivel-4-1.4.0.jar` generado
- [ ] Las clases de Niveles 1, 2 y 3 siguen accesibles desde este JAR

## Topics replicados

- [ ] `transacciones-rf1` (4 particiones, RF=1) creado
- [ ] `transacciones-rf2` (4 particiones, RF=2) creado
- [ ] `transacciones-rf3` (4 particiones, RF=3) creado
- [ ] `critical-data` (2 particiones, RF=3, min.insync.replicas=2) creado
- [ ] Todos muestran ISR completo en estado saludable

## ReplicaAnalyzer

- [ ] Se conecta al clúster via AdminClient
- [ ] Muestra correctamente Replicas e ISR por partición
- [ ] Identifica particiones saludables / degradadas / críticas
- [ ] Calcula estadísticas globales del clúster

## DurableProducer

- [ ] Ejecuta con acks=0, acks=1 y acks=all
- [ ] Muestra throughput y latencia (P50, P95, P99)
- [ ] Guarda resultados en `comparacion-acks.txt`
- [ ] Verifica mensajes en el topic después del envío

## FailoverMonitor

- [ ] Se conecta y captura estado inicial del clúster
- [ ] Detecta brokers caídos con timestamp
- [ ] Detecta cambios de leader (failover events)
- [ ] Detecta cambios en el ISR (replicas que entran/salen)
- [ ] Mide tiempo de failover en milisegundos

## ISRTracker

- [ ] Modo snapshot único funciona correctamente
- [ ] Modo tracking continuo toma snapshots periódicos
- [ ] Guarda historial en CSV para análisis posterior
- [ ] Muestra gráfica ASCII de tendencia de salud

## Experimento 08 - Réplicas básicas

- [ ] Verifiqué que `transacciones-rf3` tiene 3 réplicas por partición
- [ ] Todas las réplicas estaban in-sync en estado saludable
- [ ] Entiendo la diferencia entre `Replicas` (configurado) e `ISR` (actual)
- [ ] Entiendo que el ISR determina qué réplicas pueden ser leader

## Experimento 09 - Fallo de leader

- [ ] Ejecuté el FailoverMonitor en ventana separada antes del fallo
- [ ] Simulé el fallo del broker 2 con el script 32
- [ ] Observé el failover automático en tiempo real
- [ ] Medí el tiempo de failover: _____ ms
- [ ] El sistema continuó operando con 2 de 3 brokers
- [ ] No se perdieron mensajes con acks=all

## Experimento 10 - Durabilidad de acks

- [ ] Medí throughput con acks=0: _____ msg/s
- [ ] Medí throughput con acks=1: _____ msg/s
- [ ] Medí throughput con acks=all: _____ msg/s
- [ ] Resultados guardados en `comparacion-acks.txt`
- [ ] Entiendo el trade-off: throughput vs durabilidad

## Experimento 11 - Sincronización de réplicas

- [ ] Produje mensajes mientras el broker estaba caído
- [ ] Observé la re-sincronización automática con ISRTracker
- [ ] Medí el tiempo de re-sincronización con 5000 mensajes: _____ seg
- [ ] Confirmé que el ISR volvió a ser completo
- [ ] Entiendo que Kafka garantiza consistencia antes de agregar al ISR

## Comprensión conceptual

- [ ] Entiendo qué es una réplica y cómo difiere de una partición
- [ ] Entiendo qué es el ISR y por qué es crítico para el failover
- [ ] Entiendo que solo las réplicas in-sync pueden convertirse en leaders
- [ ] Entiendo el impacto de acks=0, acks=1 y acks=all en durabilidad
- [ ] Entiendo qué es min.insync.replicas y cómo interactúa con acks=all
- [ ] Entiendo que la re-sincronización es automática y proporcional a los datos faltantes
- [ ] Puedo decidir qué RF, min.insync.replicas y acks usar según el caso de uso

---

## Notas personales

*(Observaciones, preguntas o hallazgos durante el laboratorio)*

```

```

---

**Nivel 4 completado el:** _______________

**Tiempo total invertido:** _______________

**Tiempo de failover observado:** _____ ms

**Throughput acks=0 vs acks=all:** ___x de diferencia

**¿Qué acks usarías para una app de pagos?:** _______________

**Laboratorio Kafka completado (4 niveles):** [ ] Sí — Listo para producción 🎉
