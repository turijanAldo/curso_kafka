# Experimento 06 - Carga Balanceada entre Brokers

## Objetivo

Observar cómo el trabajo de procesar writes se distribuye automáticamente entre los 3 brokers cuando produces mensajes a un topic particionado. Verás actividad simultánea en los 3 brokers, no solo en uno.

## Hipótesis

Si produzco 600 mensajes distribuidos uniformemente entre 6 particiones, cada broker procesará exactamente 200 mensajes (1/3 del total) porque cada broker es leader de 2 de las 6 particiones.

## Pre-requisitos

- [ ] Clúster de 3 brokers corriendo
- [ ] Topic `transacciones-6p` creado (experimento 05)
- [ ] Java compilado (`mvn clean package`)
- [ ] `JAVA_HOME` configurado

---

## Procedimiento

### Paso 1 — Ver logs base de los brokers (línea base)

```powershell
.\scripts\powershell\24-ver-logs-brokers.ps1 -Lineas 20
```

Observa cuánta actividad hay actualmente en los 3 brokers. Esta es la línea base.

---

### Paso 2 — Monitorear recursos en tiempo real

Abre una **nueva ventana** de PowerShell o CMD y ejecuta:

```powershell
docker stats kafka-broker-1 kafka-broker-2 kafka-broker-3
```

**Deja esta ventana visible** durante el experimento. Verás el uso de CPU y red en tiempo real.

---

### Paso 3 — Enviar 600 mensajes con LoadBalancedProducer

En tu ventana principal, desde `kafka-lab-nivel-3\java\`:

```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-3-1.3.0.jar `
    com.nexus.kafka.nivel3.LoadBalancedProducer transacciones-6p 600 hash
```

**CMD:**
```cmd
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-3-1.3.0.jar com.nexus.kafka.nivel3.LoadBalancedProducer transacciones-6p 600 hash
```

**Mientras envía:** Observa en la ventana de `docker stats` cómo los 3 brokers muestran actividad de CPU y red **simultáneamente**.

---

### Paso 4 — Analizar el reporte del LoadBalancedProducer

Al terminar, el reporte mostrará cuántos mensajes procesó cada broker. Compara con lo esperado (200 cada uno).

---

### Paso 5 — Revisar logs post-envío

```powershell
.\scripts\powershell\24-ver-logs-brokers.ps1 -Filtro "append" -Lineas 30
```

Busca líneas que mencionen actividad de append al log de cada broker.

---

### Paso 6 — Verificar distribución con PartitionAnalyzer del Nivel 2

```powershell
& "$env:JAVA_HOME\bin\java" -cp ..\kafka-lab-nivel-2\java\target\kafka-lab-nivel-2-1.0.0.jar `
    com.nexus.kafka.nivel2.PartitionAnalyzer transacciones-6p

C:\Users\aldo_\Documents\SIIE\kafka_laboratorio\kafka-lab-nivel-3\java> & "$env:JAVA_HOME\bin\java" -cp "target\kafka-lab-nivel-3-1.3.0.jar" com.nexus.kafka.nivel3.ClusterAnalyzer transacciones-6p 


kafka-lab-nivel-3\java> & "$env:JAVA_HOME\bin\java" -cp "target\kafka-lab-nivel-3-1.3.0.jar" com.nexus.kafka.nivel3.ClusterAnalyzer transacciones-6p

```

Confirma que los 600 mensajes están distribuidos entre las 6 particiones.

---

## Resultados esperados

### Output del LoadBalancedProducer:

```
🚀 LoadBalancedProducer iniciado
📊 Analizando topic: transacciones-6p
   Particiones: 6
   Distribucion de leaders:
     Partition 0 -> Broker 1
     Partition 1 -> Broker 2
     Partition 2 -> Broker 3
     Partition 3 -> Broker 1
     Partition 4 -> Broker 2
     Partition 5 -> Broker 3

📤 Enviando 600 mensajes con estrategia: hash
  [████████████████████] 100% (600/600)

✅ Envio completado en X.X segundos
   Throughput: XXX.X mensajes/segundo

📊 Distribucion final por particion:
   Partition 0: 100 mensajes (16.67%) -> Broker 1
   Partition 1: 100 mensajes (16.67%) -> Broker 2
   Partition 2: 100 mensajes (16.67%) -> Broker 3
   Partition 3: 100 mensajes (16.67%) -> Broker 1
   Partition 4: 100 mensajes (16.67%) -> Broker 2
   Partition 5: 100 mensajes (16.67%) -> Broker 3

📈 Carga por broker:
   Broker 1: 200 mensajes (33.33%)
   Broker 2: 200 mensajes (33.33%)
   Broker 3: 200 mensajes (33.33%)

✅ Carga perfectamente balanceada entre brokers
```

### Docker stats durante el envío:

Verás picos en los 3 brokers **al mismo tiempo**:
```
NAME            CPU %    MEM USAGE    NET I/O
kafka-broker-1  15.3%    450MiB       25.1MB / 8.2MB
kafka-broker-2  14.8%    445MiB       24.8MB / 8.0MB
kafka-broker-3  15.1%    448MiB       24.9MB / 8.1MB
```

---

## Análisis

### ¿Por qué se distribuye automáticamente?

El producer no sabe nada sobre los brokers. Solo envía mensajes a un topic. Kafka internamente:

1. Calcula la partición destino: `hash(clave) % 6`
2. Consulta los metadatos del clúster para saber cuál broker es leader de esa partición
3. Envía el mensaje directamente al broker leader correspondiente

El resultado es que el trabajo se distribuye proporcionalmente a cuántas particiones tiene cada broker como leader.

### Escalabilidad horizontal

Esto demuestra el mecanismo de **escalabilidad horizontal** de Kafka:

- **1 broker:** procesa el 100% de todos los writes → cuello de botella único
- **3 brokers:** cada uno procesa ~33% → 3x más capacidad teórica
- **10 brokers:** cada uno procesa ~10% → 10x más capacidad teórica

Agregar más brokers y más particiones es todo lo que necesitas para escalar en Kafka, sin cambiar el código del producer ni del consumer.

---

## Conclusiones

- [ ] Observé actividad simultánea en los 3 brokers durante el envío
- [ ] Confirmé que cada broker procesó ~1/3 de los mensajes
- [ ] Entiendo que el balanceo es automático, sin código especial en el producer
- [ ] Entiendo que esto es la base de la escalabilidad horizontal de Kafka

## Resultados reales

### Distribución por broker observada:
| Broker | Mensajes procesados | % del total |
|---|---|---|
| Broker 1 | | |
| Broker 2 | | |
| Broker 3 | | |

### Throughput observado: ___ mensajes/segundo

### ¿Los 3 brokers mostraron actividad simultánea en docker stats? [ ] Sí / [ ] No
