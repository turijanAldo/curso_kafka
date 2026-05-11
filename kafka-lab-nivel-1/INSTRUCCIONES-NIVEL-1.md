# Instrucciones Detalladas - Laboratorio Kafka Nivel 1

## Sección 1: Preparación del entorno

### 1.1 Verificar requisitos del sistema

Abre **PowerShell** y ejecuta:

```powershell
.\scripts\powershell\00-verificar-requisitos.ps1
```

O en **CMD**:
```cmd
scripts\cmd\00-verificar-requisitos.bat
```

El script verificará Docker, los puertos 9092/9093, memoria disponible y WSL2.

**Resultado esperado:** Todos los checks en verde. Si alguno aparece en rojo, corrige el error antes de continuar.

### 1.2 Configurar política de ejecución de PowerShell (solo primera vez)

Si PowerShell bloquea la ejecución de scripts:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

---

## Sección 2: Inicio del clúster

### 2.1 Levantar Kafka

```powershell
.\scripts\powershell\01-iniciar-kafka.ps1
```

El script:
1. Verifica que `docker/docker-compose.yml` existe
2. Ejecuta `docker-compose up -d` en el directorio `docker/`
3. Espera hasta 60 segundos verificando que el puerto 9092 responde
4. Muestra el estado del contenedor y los últimos logs

**Resultado esperado:**
```
[  OK  ] Kafka respondiendo en localhost:9092
KAFKA INICIADO CORRECTAMENTE
  Broker accesible en: localhost:9092
```

### 2.2 Verificar que el contenedor corre

```powershell
docker ps --filter "name=kafka-nivel1"
```

Deberías ver el contenedor con estado `Up X seconds (healthy)`.

---

## Sección 3: Verificación del clúster

### 3.1 Ejecutar script de verificación completa

```powershell
.\scripts\powershell\02-verificar-cluster.ps1
```

El script ejecuta dentro del contenedor:
- `kafka-broker-api-versions.sh` — versiones de API soportadas
- `kafka-topics.sh --list` — topics existentes
- `kafka-configs.sh --describe` — configuración del broker
- `kafka-metadata-quorum.sh describe --status` — estado KRaft

### 3.2 Output esperado del quorum KRaft

```
ClusterId:              MkU3OEVBNTcwNTJENDM2Qk
LeaderId:               1
LeaderEpoch:            1
HighWatermark:          ...
MaxFollowerLag:         0
CurrentVoters:          [{"nodeId":1,...}]
CurrentObservers:       []
```

Esto confirma que el nodo 1 es el líder del quorum KRaft.

---

## Sección 4: Compilación del código Java

### 4.1 Verificar que Java y Maven están instalados

```powershell
java --version
mvn --version
```

Necesitas Java 17+ y Maven 3.8+.

### 4.2 Compilar el proyecto

```powershell
cd java
mvn clean package
```

La primera vez Maven descargará dependencias (~5-10 min según la conexión).

**Output esperado al final:**
```
[INFO] BUILD SUCCESS
[INFO] Total time: XX s
```

### 4.3 Verificar el JAR generado

```powershell
ls target\kafka-lab-nivel-1-1.0.0.jar
```

El archivo pesa ~15-20 MB (fat jar con todas las dependencias incluidas).

---

## Sección 5: Ejecución del Experimento 01

Sigue la guía completa en `experimentos/exp-01-primer-mensaje.md`.

### Resumen rápido:

**Terminal 1 — Consumer:**
```powershell
cd java
java -cp target\kafka-lab-nivel-1-1.0.0.jar com.nexus.kafka.nivel1.SimpleConsumer primer-topic grupo-prueba
```

**Terminal 2 — Producer:**
```powershell
cd java
java -cp target\kafka-lab-nivel-1-1.0.0.jar com.nexus.kafka.nivel1.SimpleProducer primer-topic "Hola Kafka desde Nivel 1"
```

Observa cómo el mensaje aparece en la Terminal 1 inmediatamente.

Para detener el consumer: `Ctrl+C` en la Terminal 1.

---

## Sección 6: Exploración adicional

### Listar todos los topics

```powershell
docker exec kafka-nivel1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --list
```

### Describir un topic en detalle

```powershell
docker exec kafka-nivel1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --describe `
    --topic primer-topic
```

### Ver los mensajes almacenados (consumer de consola nativo)

```powershell
docker exec -it kafka-nivel1 /opt/kafka/bin/kafka-console-consumer.sh `
    --bootstrap-server localhost:9092 `
    --topic primer-topic `
    --from-beginning
```

Presiona `Ctrl+C` para salir.

### Ver grupos de consumers activos

```powershell
docker exec kafka-nivel1 /opt/kafka/bin/kafka-consumer-groups.sh `
    --bootstrap-server localhost:9092 `
    --list
```

### Ver offsets de un grupo de consumers

```powershell
docker exec kafka-nivel1 /opt/kafka/bin/kafka-consumer-groups.sh `
    --bootstrap-server localhost:9092 `
    --describe `
    --group grupo-prueba
```

### Ver configuración completa del broker

```powershell
docker exec kafka-nivel1 /opt/kafka/bin/kafka-configs.sh `
    --bootstrap-server localhost:9092 `
    --describe `
    --entity-type brokers `
    --entity-name 1 `
    --all
```

---

## Sección 7: Limpieza

### 7.1 Detener el clúster (conservando datos)

```powershell
.\scripts\powershell\03-detener-kafka.ps1
```

Los datos persisten en el volumen Docker. Al volver a ejecutar `01-iniciar-kafka.ps1`, los topics y mensajes siguen ahí.

### 7.2 Reiniciar el clúster

```powershell
.\scripts\powershell\01-iniciar-kafka.ps1
```

### 7.3 Limpiar todo (elimina datos irreversiblemente)

```powershell
.\scripts\powershell\04-limpiar-todo.ps1
```

Pedirá confirmación escribiendo `SI`. Elimina el volumen Docker y limpia los directorios de logs.

---

## Sección 8: Próximos pasos — Nivel 2

En el **Nivel 2** explorarás:

- **Particiones**: cómo Kafka divide un topic en múltiples particiones para paralelismo
- **Consumer groups**: cómo múltiples consumers se reparten las particiones
- **Clúster multi-broker**: añadir más brokers para alta disponibilidad
- **Replication factor > 1**: cómo Kafka replica mensajes entre brokers
- **Producers con keys**: garantizar orden dentro de una partición

La comprensión del offset y la partition que adquiriste en el Nivel 1 es el fundamento de todo lo anterior.
