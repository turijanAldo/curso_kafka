# Instrucciones Detalladas - Laboratorio Kafka Nivel 2

## Sección 1: Preparación

### 1.1 Verificar que el Nivel 1 funciona

Antes de empezar, confirma que el broker del Nivel 1 está activo:

```powershell
docker ps --filter "name=kafka-nivel1"
```

Si no está corriendo, inícialo:
```powershell
cd ..\kafka-lab-nivel-1
.\scripts\powershell\01-iniciar-kafka.ps1
cd ..\kafka-lab-nivel-2
```

### 1.2 Verificar JAVA_HOME

Los scripts y comandos de este nivel usan `%JAVA_HOME%\bin\java` en lugar de `java` directamente:

**PowerShell:**
```powershell
echo $env:JAVA_HOME
# Si está vacío, configurarlo:
$env:JAVA_HOME = "C:\Program Files\Java\jdk-17"   # Ajusta la ruta a tu instalación
```

**CMD:**
```cmd
echo %JAVA_HOME%
:: Si está vacío:
set "JAVA_HOME=C:\Program Files\Java\jdk-17"
```

Para que persista en futuras sesiones, agrégalo a las **Variables de entorno del sistema** de Windows.

---

## Sección 2: Creación de topics particionados

### 2.1 Crear los tres topics

```powershell
.\scripts\powershell\10-crear-topics-particionados.ps1

#comando 1 a 1 borrar si existen 
docker exec kafka-nivel1 /opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--delete \
--topic transacciones-1p

docker exec kafka-nivel1 /opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--delete \
--topic transacciones-4p

docker exec kafka-nivel1 /opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--delete \
--topic transacciones-8p
```


🏗️ 4. Crear los topics
🔹 Topic con 1 partición (máximo orden)


```bash
docker exec kafka-nivel1 /opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--create \
--topic transacciones-1p \
--partitions 1 \
--replication-factor 1
```

🔹 Topic con 4 particiones (balance)
bash

```bash
docker exec kafka-nivel1 /opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--create \
--topic transacciones-4p \
--partitions 4 \
--replication-factor 1
```

🔹 Topic con 8 particiones (máximo paralelismo)
bash

```bash
docker exec kafka-nivel1 /opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--create \
--topic transacciones-8p \
--partitions 8 \
--replication-factor 1
```

🔍 5. Ver detalle de cada topic
```bash
docker exec kafka-nivel1 /opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--describe \
--topic transacciones-1p
```

(Repite para los otros)
📊 6. Ver todos los topics al final

```bash
docker exec kafka-nivel1 /opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--list
```

🧠 Lo que debes observar (esto es lo importante)
```bash
    transacciones-1p → solo 1 consumer puede procesar

    transacciones-4p → hasta 4 consumers en paralelo

    transacciones-8p → hasta 8 consumers
```

Esto crea:
- `transacciones-1p` → 1 partición
- `transacciones-4p` → 4 particiones  
- `transacciones-8p` → 8 particiones

**Resultado esperado:** `[  OK  ] Topic 'transacciones-Xp' creado con X particion(es)`

### 2.2 Verificar particiones

```powershell
.\scripts\powershell\11-describir-particiones.ps1
```

Esto muestra para cada topic: el número de particiones, el broker líder y los offsets disponibles.

**Punto clave:** Con 1 broker, el campo `Leader` siempre será `1`. En producción con 3 brokers, verías valores diferentes → distribución de carga entre brokers.

---

## Sección 3: Compilación del código

### 3.1 Compilar el proyecto

```powershell
cd java
mvn clean package
cd ..
```

La primera vez descarga dependencias (~60-120 seg). Las siguientes veces es mucho más rápido.

**Resultado esperado:**
```
[INFO] BUILD SUCCESS
```

### 3.2 Verificar clases disponibles en el JAR

El fat JAR `target\kafka-lab-nivel-2-1.0.0.jar` contiene:

| Clase | Uso |
|---|---|
| `com.nexus.kafka.nivel1.SimpleProducer` | Del Nivel 1 |
| `com.nexus.kafka.nivel1.SimpleConsumer` | Del Nivel 1 |
| `com.nexus.kafka.nivel2.KeyedProducer` | Nuevo: producer con clave |
| `com.nexus.kafka.nivel2.InstrumentedConsumer` | Nuevo: consumer con rebalanceo visible |
| `com.nexus.kafka.nivel2.BatchProducer` | Nuevo: producer de alto volumen |
| `com.nexus.kafka.nivel2.PartitionAnalyzer` | Nuevo: análisis de particiones |

### 3.3 RECORDATORIO: Siempre usar JAVA_HOME

```powershell
# ✅ Correcto
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar com.nexus.kafka.nivel2.KeyedProducer ...

# ⚠️ Puede funcionar pero depende del PATH
java -cp target\kafka-lab-nivel-2-1.0.0.jar com.nexus.kafka.nivel2.KeyedProducer ...
```

---

## Sección 4: Experimento 02 - Distribución por clave

Sigue el archivo `experimentos\exp-02-distribucion-por-clave.md` paso a paso.

**Objetivo concreto:** comprobar que `user-123` siempre va a la misma partición, sin importar cuántas veces lo envíes.

**Comando clave a memorizar:**
```powershell
& "$env:JAVA_HOME\bin\java" -cp java\target\kafka-lab-nivel-2-1.0.0.jar `
    com.nexus.kafka.nivel2.KeyedProducer transacciones-4p user-123 "mi-mensaje"
```

---

## Sección 5: Experimento 03 - Paralelismo de consumers

Sigue `experimentos\exp-03-paralelismo-consumers.md`.

**Lo más importante de este experimento:** ver el rebalanceo en tiempo real. Para eso:

1. Ejecuta el script automático que abre 4 ventanas:
   ```powershell
   .\scripts\powershell\13-consumer-group-multiple.ps1
   ```
2. Observa el mensaje `🎯 ASIGNADO A: Partitions [X]` en cada ventana
3. Cierra una ventana con Ctrl+C
4. Observa cómo las otras ventanas muestran `⚠️ REVOCADAS` y luego `🎯 ASIGNADO A` con más particiones

---

## Sección 6: Experimento 04 - Throughput

Sigue `experimentos\exp-04-particiones-vs-throughput.md`.

Para enviar mensajes masivos usa el BatchProducer:
```powershell
& "$env:JAVA_HOME\bin\java" -cp java\target\kafka-lab-nivel-2-1.0.0.jar `
    com.nexus.kafka.nivel2.BatchProducer transacciones-4p 1000 true

#
& "$env:JAVA_HOME\bin\java" -cp "target\kafka-lab-nivel-2-1.0.0.jar" com.nexus.kafka.nivel2.BatchProducer transacciones-4p 10000 true
```

Al final, analiza la distribución:
```powershell
& "$env:JAVA_HOME\bin\java" -cp java\target\kafka-lab-nivel-2-1.0.0.jar `
    com.nexus.kafka.nivel2.PartitionAnalyzer transacciones-4p

    #
    & "$env:JAVA_HOME\bin\java" -cp "target\kafka-lab-nivel-2-1.0.0.jar" com.nexus.kafka.nivel2.PartitionAnalyzer transacciones-4p
```

---

## Sección 7: Limpieza

### Detener consumers

En cada ventana de consumer, presiona `Ctrl+C`. El shutdown hook imprimirá el resumen de mensajes procesados.

### Eliminar topics (opcional)

```powershell
$topics = @("transacciones-1p","transacciones-4p","transacciones-8p")
foreach ($t in $topics) {
    docker exec kafka-nivel1 /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server localhost:9092 `
        --delete `
        --topic $t
}
```

### Detener el broker (si ya no lo necesitas)

```powershell
cd ..\kafka-lab-nivel-1
.\scripts\powershell\03-detener-kafka.ps1
```

---

## Sección 8: Próximos pasos — Nivel 3

En el **Nivel 3** explorarás el clúster multi-broker:

- **Alta disponibilidad**: qué pasa cuando un broker falla
- **Replication factor > 1**: cómo Kafka copia los datos entre brokers
- **Leader election**: cómo un follower se convierte en líder cuando el líder muere
- **ISR (In-Sync Replicas)**: qué significa y por qué importa para la durabilidad
- **min.insync.replicas**: el parámetro que controla el trade-off entre durabilidad y disponibilidad

Necesitarás cambiar el `docker-compose.yml` para levantar **3 brokers** en lugar de 1.
