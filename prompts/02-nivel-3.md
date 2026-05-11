# LABORATORIO KAFKA - NIVEL 3: Clúster Multi-Broker y Distribución

## Contexto del proyecto

Este es el Nivel Tres del laboratorio Kafka, donde pasamos de un solo broker a un clúster completo de tres brokers. Aquí verás cómo Kafka distribuye automáticamente las particiones entre múltiples servidores y cómo esta distribución mejora tanto el rendimiento como la disponibilidad del sistema.

**Objetivo de aprendizaje:** Entender cómo un clúster multi-broker distribuye la carga de trabajo, cómo se asignan los leaders de particiones entre los brokers y cómo múltiples brokers mejoran el throughput del sistema mediante la paralelización del almacenamiento y procesamiento.

**Pre-requisito:** Nivel Dos completado y entendido. Debes tener clara la diferencia entre particiones y el concepto de paralelismo de consumers.

**Nota importante:** Este nivel sigue usando Kafka 4.0+ con KRaft (sin ZooKeeper) y mantiene la compatibilidad con Windows 10 usando `%JAVA_HOME%\bin\java` para ejecutar aplicaciones.

---

## Requisitos previos

Antes de comenzar el Nivel Tres, necesitas tener el Nivel Uno y Nivel Dos completados correctamente. El clúster del Nivel Uno debe poder iniciarse y detenerse sin problemas. Las aplicaciones Java del Nivel Dos deben compilar y ejecutarse correctamente. Tu variable de entorno JAVA_HOME debe estar configurada apropiadamente en Windows. Docker Desktop debe tener al menos seis gigabytes de RAM asignados porque ahora levantaremos tres brokers simultáneamente en lugar de uno solo.

---

## Estructura de directorios a AGREGAR sobre niveles anteriores

La estructura que construiremos en este nivel se integra con lo que ya tienes de los niveles previos. Vamos a mantener todo el código Java existente porque seguirá funcionando, solo que ahora se conectará a un clúster de tres brokers en lugar de uno. Los experimentos nuevos se enfocarán en observar cómo se comporta la distribución cuando hay múltiples servidores disponibles.

kafka-lab-nivel-3/
├── docker/
│   ├── docker-compose-cluster.yml (NUEVO - reemplaza temporalmente el anterior)
│   └── kafka-config/
│       ├── broker-1.properties
│       ├── broker-2.properties
│       └── broker-3.properties
├── scripts/
│   ├── powershell/
│   │   ├── 20-iniciar-cluster.ps1
│   │   ├── 21-verificar-cluster.ps1
│   │   ├── 22-describir-distribucion.ps1
│   │   ├── 23-detener-cluster.ps1
│   │   └── 24-ver-logs-brokers.ps1
│   └── cmd/
│       ├── 20-iniciar-cluster.bat
│       ├── 21-verificar-cluster.bat
│       ├── 22-describir-distribucion.bat
│       ├── 23-detener-cluster.bat
│       └── 24-ver-logs-brokers.bat
├── experimentos/
│   ├── exp-05-distribucion-leaders.md
│   ├── exp-06-carga-balanceada.md
│   ├── exp-07-throughput-multibroker.md
│   └── resultados/
│       ├── distribucion-leaders.txt
│       ├── asignacion-brokers.txt
│       └── metricas-throughput.txt
├── java/
│   ├── pom.xml (ACTUALIZAR)
│   └── src/
│       └── main/
│           └── java/
│               └── com/
│                   └── nexus/
│                       └── kafka/
│                           └── nivel3/
│                               ├── ClusterAnalyzer.java
│                               ├── LoadBalancedProducer.java
│                               └── ThroughputBenchmark.java
└── INSTRUCCIONES-NIVEL-3.md

---

## Archivos a generar

### 1. docker/docker-compose-cluster.yml

Este es el archivo más importante del Nivel Tres porque define nuestro clúster de tres brokers. A diferencia del docker-compose simple que teníamos antes con un solo broker, este archivo configura tres contenedores independientes que trabajan coordinadamente como un clúster unificado.

Cada broker en el clúster necesita su propio identificador único llamado KAFKA_NODE_ID. El broker uno tendrá el ID uno, el broker dos tendrá el ID dos y el broker tres tendrá el ID tres. Todos los brokers deben conocerse entre sí a través de la configuración de KAFKA_CONTROLLER_QUORUM_VOTERS, que es una lista especial que indica dónde están los otros nodos del clúster. En KRaft (la arquitectura sin ZooKeeper), cada broker puede actuar tanto como broker de datos como controlador del clúster, por lo que configuramos KAFKA_PROCESS_ROLES como "broker,controller" para los tres nodos.

Los puertos externos para acceder a cada broker desde Windows serán nueve cero nueve dos para el broker uno, nueve cero nueve tres para el broker dos y nueve cero nueve cuatro para el broker tres. Internamente dentro de la red Docker, cada broker también tiene su propio puerto de controlador en diecinueve cero nueve dos, diecinueve cero nueve tres y diecinueve cero nueve cuatro respectivamente.

Cada broker debe tener su propio volumen persistente separado para almacenar datos. Nombra estos volúmenes como kafka-data-1, kafka-data-2 y kafka-data-3 para que sea claro a qué broker pertenece cada uno. Esto es crítico porque si los tres brokers compartieran el mismo volumen, tendrías corrupción de datos garantizada.

La configuración de KAFKA_ADVERTISED_LISTENERS debe incluir dos listeners para cada broker. El listener interno para comunicación entre brokers dentro de Docker usará el hostname del contenedor como kafka-broker-1:29092. El listener externo para que tus aplicaciones Java se conecten desde Windows usará localhost con el puerto correspondiente como localhost:9092.

Un aspecto importante es que ahora podemos configurar el factor de replicación para los topics internos de Kafka. Configura KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR igual a tres porque ahora tenemos tres brokers disponibles. Esto significa que el topic especial donde Kafka guarda los offsets de los consumer groups estará replicado en los tres brokers, haciéndolo más robusto. También configura KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR igual a tres y KAFKA_TRANSACTION_STATE_LOG_MIN_ISR igual a dos para garantizar durabilidad de transacciones.

Agrega health checks a cada contenedor que verifiquen que el broker está respondiendo correctamente. El comando de health check debe ejecutar kafka-broker-api-versions para probar que el broker está funcional. Configura un intervalo de treinta segundos, timeout de diez segundos y tres reintentos antes de marcar el contenedor como no saludable.

Incluye comentarios abundantes explicando cada sección de configuración. Por ejemplo, explica por qué KAFKA_CONTROLLER_QUORUM_VOTERS tiene ese formato específico de "1@kafka-broker-1:19092,2@kafka-broker-2:19093,3@kafka-broker-3:19094" donde el número antes del arroba es el node ID y después del arroba es el hostname y puerto del controlador.

### 2. scripts/powershell/20-iniciar-cluster.ps1

Este script es la puerta de entrada al clúster multi-broker. Su trabajo es iniciar los tres brokers de forma coordinada y verificar que todos están funcionando correctamente antes de declarar éxito.

Primero, el script debe verificar que estamos en el directorio correcto navegando hasta la carpeta docker donde está el archivo docker-compose-cluster.yml. Verifica que ese archivo existe antes de intentar usarlo. Si no existe, muestra un mensaje de error claro indicando que probablemente estamos en el directorio equivocado.

Antes de iniciar los contenedores, verifica que los puertos nueve cero nueve dos, nueve cero nueve tres y nueve cero nueve cuatro están disponibles. Usa Test-NetConnection para cada puerto. Si algún puerto está ocupado, muestra un mensaje específico indicando qué puerto está en uso y sugiere cómo liberar ese puerto o cambiar la configuración.

Ejecuta docker-compose usando el archivo específico con el parámetro menos f docker-compose-cluster.yml seguido de up menos d para modo detached. Captura el código de salida para verificar que el comando se ejecutó exitosamente.

Después de iniciar los contenedores, implementa un loop de espera inteligente. No simplemente esperes un tiempo fijo como treinta segundos, sino que verifica activamente cada cinco segundos si los tres brokers están listos. Para verificar esto, ejecuta docker ps y filtra por contenedores que contengan "kafka-broker" en su nombre y tengan status "healthy" o "Up". Cuando los tres brokers estén activos, continúa al siguiente paso.

Una vez que los brokers están corriendo, ejecuta un comando de verificación dentro de cada contenedor para listar los brokers del clúster usando kafka-broker-api-versions. Esto confirma no solo que cada contenedor está corriendo sino que cada broker puede comunicarse con los otros y conoce la topología completa del clúster.

Muestra un resumen visual del clúster usando Write-Host con colores. Lista cada broker con su ID, hostname, puerto externo y estado. Por ejemplo:

Finalmente, muestra sugerencias de próximos pasos como ejecutar el script de verificación del clúster o crear un topic particionado para ver la distribución.

### 3. scripts/powershell/21-verificar-cluster.ps1

Este script proporciona una visión completa del estado del clúster en un momento dado. Es como una fotografía detallada de cómo está configurado y funcionando el clúster.

Comienza mostrando información básica del clúster ejecutando kafka-metadata --snapshot dentro del primer broker. Este comando muestra el estado interno del clúster incluyendo qué brokers están registrados y cuál es el controlador activo.

Luego ejecuta kafka-broker-api-versions contra localhost:9092 para obtener la lista completa de brokers desde la perspectiva del broker uno. Parsea la salida para extraer los IDs de los brokers y sus direcciones. Muestra esta información en formato de tabla.

A continuación, lista todos los topics existentes ejecutando kafka-topics --list. Si no hay topics todavía, muestra un mensaje informativo indicando que el clúster está limpio y sugiere crear algunos topics para experimentar.

Para cada topic que exista, ejecuta kafka-topics --describe para mostrar cómo están distribuidas sus particiones. Esta es la parte más interesante porque aquí verás visualmente cómo Kafka distribuyó los leaders de las particiones entre los tres brokers.

Parsea la salida del describe para crear una representación visual. Por ejemplo, si tienes un topic con seis particiones, muestra algo como:

📊 Topic: transacciones-6p
Particiones: 6
Replication Factor: 1Partition 0 → Leader: Broker 1
Partition 1 → Leader: Broker 2
Partition 2 → Leader: Broker 3
Partition 3 → Leader: Broker 1
Partition 4 → Leader: Broker 2
Partition 5 → Leader: Broker 3Distribución de leaders:
Broker 1: 2 particiones (33.3%)
Broker 2: 2 particiones (33.3%)
Broker 3: 2 particiones (33.3%)
✅ Balanceado correctamente

Incluye una sección que verifique el uso de recursos de cada contenedor usando docker stats --no-stream. Muestra CPU, memoria y uso de red para cada broker. Esto es útil para identificar si algún broker está sobrecargado comparado con los otros.

### 4. scripts/powershell/22-describir-distribucion.ps1

Este script está enfocado específicamente en analizar cómo se distribuyen las particiones y leaders entre los brokers. Es una versión más profunda y analítica de la verificación del clúster.

Acepta un parámetro opcional de nombre de topic. Si se proporciona, analiza solo ese topic. Si no se proporciona, analiza todos los topics existentes.

Para cada topic, extrae información detallada sobre cada partición incluyendo quién es el leader, dónde están las réplicas (aunque en este nivel todavía no tenemos réplicas múltiples, el script debe estar preparado para mostrarlas) y cuál es el ISR (in-sync replicas).

Calcula estadísticas de distribución. Cuenta cuántas particiones tiene cada broker como leader y calcula el porcentaje. Verifica si la distribución es balanceada. Una distribución perfectamente balanceada significa que cada broker tiene aproximadamente el mismo número de particiones como leader. Por ejemplo, con doce particiones y tres brokers, cada broker debería ser leader de cuatro particiones.

Si detectas desbalanceo, calcula la desviación. Por ejemplo, si el broker uno tiene seis particiones, el broker dos tiene cuatro y el broker tres tiene dos, hay un desbalanceo significativo. Muestra una advertencia indicando este desbalanceo y sugiere ejecutar una reasignación de particiones si es necesario.

Crea una visualización ASCII de la distribución. Podrías usar caracteres de barras para representar visualmente cuántas particiones maneja cada broker. Por ejemplo:

Distribución de particiones como leader:Broker 1: ████████ (8 particiones)
Broker 2: ████████ (8 particiones)
Broker 3: ████████ (8 particiones)Balance: ✅ Perfecto

Guarda los resultados en experimentos/resultados/distribucion-leaders.txt para referencia futura. Incluye un timestamp para saber cuándo se generó este reporte.

### 5. scripts/powershell/23-detener-cluster.ps1

Script para detener el clúster de forma limpia y ordenada. Es importante detener el clúster correctamente para evitar corrupción de datos o problemas al reiniciar.

Primero muestra un mensaje informativo explicando que vas a detener los tres brokers pero que los datos permanecerán en los volúmenes Docker para cuando reinicies el clúster.

Ejecuta docker-compose -f docker-compose-cluster.yml down. Captura y muestra la salida para que veas cada contenedor deteniéndose.

Verifica que todos los contenedores efectivamente se detuvieron listando contenedores con docker ps y filtrando por kafka. Si encuentras algún contenedor de Kafka todavía corriendo, muestra una advertencia.

Opcionalmente, ofrece al usuario la posibilidad de ver los volúmenes que quedaron persistidos con docker volume ls filtrando por kafka. Esto le recuerda al usuario dónde están sus datos guardados.

Muestra un mensaje final confirmando que el clúster se detuvo exitosamente y recordando cómo reiniciarlo usando el script veinte-iniciar-cluster.

### 6. scripts/powershell/24-ver-logs-brokers.ps1

Este script facilita la inspección de logs de los brokers, lo cual es esencial para debugging y entender qué está pasando internamente en el clúster.

Acepta parámetros opcionales: cuál broker inspeccionar (uno, dos, tres o todos) y cuántas líneas de log mostrar (por defecto cincuenta).

Si el usuario no especifica un broker, muestra los logs de los tres brokers en secciones separadas claramente marcadas. Para cada broker, ejecuta docker logs kafka-broker-N --tail 50 donde N es el número del broker.

Resalta líneas importantes en los logs usando colores. Por ejemplo, líneas que contienen "ERROR" en rojo, líneas con "WARN" en amarillo y líneas con "Leader election" o "Partition assignment" en cyan porque son especialmente interesantes para entender la distribución del clúster.

Incluye una opción para seguir los logs en tiempo real (modo follow) agregando el parámetro --follow al comando docker logs. Cuando estés en modo follow, muestra una nota indicando que presiones Ctrl+C para salir.

Opcionalmente, permite filtrar los logs por palabra clave. Por ejemplo, el usuario podría querer ver solo líneas relacionadas con "partition" o "replication". Usa Select-String para filtrar la salida.

### 7. scripts/cmd/*.bat

Genera versiones batch equivalentes de todos los scripts PowerShell anteriores. Estos scripts deben proporcionar la misma funcionalidad pero usando sintaxis de batch de Windows.

La verificación de puertos en batch es más compleja que en PowerShell. Usa netstat -ano | findstr LISTENING | findstr :9092 para verificar si el puerto está en uso. El código de retorno te indicará si encontró algo.

Para mostrar colores en CMD, puedes intentar usar caracteres especiales o simplemente usar echo normal sin colores si los colores son complicados de implementar en batch.

Los loops y condicionales deben traducirse a la sintaxis de batch. Por ejemplo, un loop while en PowerShell se convierte en un goto-label loop en batch.

Recuerda que en batch las variables se acceden con signos de porcentaje dobles como %%variable%% dentro de loops y %variable% fuera de loops.

Incluye comentarios REM explicando cada sección para que el script sea fácil de entender y mantener.

### 8. java/pom.xml (ACTUALIZADO)

Actualiza el pom.xml para incluir las nuevas clases del Nivel Tres. Agrega dependencias adicionales si son necesarias, aunque probablemente las dependencias existentes de kafka-clients sean suficientes.

Asegúrate de que la configuración del maven-shade-plugin incluya las nuevas clases main como puntos de entrada ejecutables. Cada clase con método main debe poder generar su propio JAR ejecutable o al menos ser ejecutable usando java -cp.

Incrementa la versión del proyecto a uno punto tres punto cero para reflejar que estamos en el Nivel Tres del laboratorio.

Incluye un comentario en el archivo indicando qué clases nuevas se agregaron en este nivel y para qué sirve cada una.

### 9. java/.../ClusterAnalyzer.java

Esta clase proporciona análisis programático del estado del clúster desde Java. Es similar al script de verificación pero desde código, lo cual te permite integrar este análisis en aplicaciones más complejas si fuera necesario.

Usa AdminClient de Kafka para conectarse al clúster. Configura el bootstrap servers con los tres brokers: "localhost:9092,localhost:9093,localhost:9094". Aunque técnicamente solo necesitas especificar uno porque Kafka automáticamente descubre los otros, especificar los tres es una mejor práctica para alta disponibilidad.

Implementa un método describeCluster que ejecute adminClient.describeCluster y procese el resultado. Este método debe extraer información como el cluster ID, el controlador actual y la lista de todos los nodos (brokers).

Para cada nodo en el clúster, muestra su ID, host y puerto. Indica claramente cuál es el controlador activo del clúster marcándolo con un símbolo especial.

Implementa un método analyzeTopicDistribution que acepte un nombre de topic como parámetro. Este método debe usar adminClient.describeTopics para obtener los detalles de las particiones del topic.

Para cada partición del topic, extrae quién es el leader, cuáles son las réplicas y cuál es el ISR. Calcula estadísticas de distribución similar a lo que hace el script PowerShell, pero todo desde código Java.

Genera un reporte en formato de texto que se puede imprimir en consola o guardar en archivo. El reporte debe incluir:

╔════════════════════════════════════════════════════════╗
║          ANÁLISIS DEL CLÚSTER KAFKA                    ║
╚════════════════════════════════════════════════════════╝Cluster ID: xJ9kL3mN4pQ5rS6tVw8xY9zAControlador activo: Broker 2 (localhost:9093)Brokers en el clúster:
• Broker 1: localhost:9092
• Broker 2: localhost:9093 ⭐ (Controlador)
• Broker 3: localhost:9094─────────────────────────────────────────────────────────Topic: transacciones-6pDistribución de particiones:
Partition 0 → Leader: Broker 1 | Replicas: [1] | ISR: [1]
Partition 1 → Leader: Broker 2 | Replicas: [2] | ISR: [2]
Partition 2 → Leader: Broker 3 | Replicas: [3] | ISR: [3]
Partition 3 → Leader: Broker 1 | Replicas: [1] | ISR: [1]
Partition 4 → Leader: Broker 2 | Replicas: [2] | ISR: [2]
Partition 5 → Leader: Broker 3 | Replicas: [3] | ISR: [3]Estadísticas de distribución de leaders:
Broker 1: 2 particiones (33.33%)
Broker 2: 2 particiones (33.33%)
Broker 3: 2 particiones (33.33%)✅ Distribución balanceada correctamente

Implementa manejo robusto de errores. Si el clúster no está disponible, muestra un mensaje claro. Si el topic no existe, informa de eso específicamente en lugar de lanzar una excepción críptica.

Incluye un método main que acepte argumentos de línea de comandos usando Apache Commons CLI. Permite al usuario especificar qué topic analizar o si quiere un análisis general del clúster sin enfocarse en un topic específico.

### 10. java/.../LoadBalancedProducer.java

Este producer está diseñado para demostrar cómo el trabajo se distribuye automáticamente entre múltiples brokers cuando produces mensajes a un topic particionado.

Configura el producer con bootstrap servers apuntando a los tres brokers. Usa un partitioner personalizado o el por defecto (que usa hash de la clave) para que puedas experimentar con diferentes estrategias de distribución.

Acepta parámetros de línea de comandos: nombre del topic, número de mensajes a enviar y estrategia de keys (secuencial, random o hash).

Si la estrategia es secuencial, genera claves como "key-001", "key-002", etcétera. Si es random, genera claves aleatorias. Si es hash, usa una distribución hash uniforme para garantizar balance perfecto.

Antes de enviar mensajes, consulta los metadatos del topic usando el producer mismo. El producer internamente mantiene metadata sobre el clúster. Accede a estos metadatos para mostrar cuántas particiones tiene el topic y en qué brokers están los leaders.

Mientras envías mensajes, mantén contadores de cuántos mensajes fueron a cada partición. Al final, muestra un reporte de distribución.

Mide también el tiempo total de envío y calcula mensajes por segundo. Compara este throughput con el que obtendrías en un solo broker para demostrar la mejora de rendimiento con múltiples brokers.

Implementa callbacks para el método send de forma que puedas imprimir en tiempo real cada mensaje enviado mostrando a qué partición y a qué broker (implícitamente, el broker leader de esa partición) fue el mensaje.

Output esperado durante ejecución:

🚀 LoadBalancedProducer iniciado📊 Analizando topic: transacciones-6p
Particiones: 6
Distribución de leaders:
Partition 0 → Broker 1
Partition 1 → Broker 2
Partition 2 → Broker 3
Partition 3 → Broker 1
Partition 4 → Broker 2
Partition 5 → Broker 3📤 Enviando 600 mensajes con estrategia: hash[████████████████████] 100% (600/600)✅ Envío completado en 3.2 segundos
Throughput: 187.5 mensajes/segundo📊 Distribución final:
Partition 0: 100 mensajes (16.67%) → Broker 1
Partition 1: 100 mensajes (16.67%) → Broker 2
Partition 2: 100 mensajes (16.67%) → Broker 3
Partition 3: 100 mensajes (16.67%) → Broker 1
Partition 4: 100 mensajes (16.67%) → Broker 2
Partition 5: 100 mensajes (16.67%) → Broker 3📈 Carga por broker:
Broker 1: 200 mensajes (33.33%)
Broker 2: 200 mensajes (33.33%)
Broker 3: 200 mensajes (33.33%)✅ Carga perfectamente balanceada entre brokers

### 11. java/.../ThroughputBenchmark.java

Esta clase implementa un benchmark más sofisticado que mide el throughput del clúster bajo diferentes configuraciones.

Permite configurar parámetros del benchmark: número de mensajes, tamaño del mensaje en bytes, número de threads productores concurrentes, batch size y linger time.

Implementa múltiples threads productores que envían mensajes simultáneamente. Cada thread debe tener su propia instancia de KafkaProducer (o puedes compartir una instancia que es thread-safe).

Mide no solo el throughput general sino también métricas más detalladas como latencia percentil noventa y cinco, latencia percentil noventa y nueve y latencia máxima de envío.

Usa un ExecutorService con un pool de threads. Cada thread ejecuta una tarea que envía un número específico de mensajes y registra el tiempo que tomó cada envío.

Acumula todas las latencias en una lista que luego ordenarás para calcular percentiles. También mantén contadores atómicos para throughput total.

Al finalizar el benchmark, genera un reporte detallado:

╔═══════════════════════════════════════════════════════╗
║         BENCHMARK DE THROUGHPUT - KAFKA CLUSTER       ║
╚═══════════════════════════════════════════════════════╝
Configuración del test:
• Topic: transacciones-6p (6 particiones)
• Mensajes totales: 10,000
• Tamaño del mensaje: 1 KB
• Threads productores: 3
• Batch size: 100
• Linger ms: 10
─────────────────────────────────────────────────────────
Resultados:
Throughput:
• Mensajes/segundo: 3,125
• MB/segundo: 3.05
Latencia (ms):
• Promedio: 24.5
• Percentil 50 (mediana): 22.0
• Percentil 95: 45.0
• Percentil 99: 67.0
• Máxima: 156.0
Distribución por broker:
• Broker 1: 3,334 mensajes (33.34%)
• Broker 2: 3,333 mensajes (33.33%)
• Broker 3: 3,333 mensajes (33.33%)
✅ Benchmark completado exitosamente


Guarda los resultados en un archivo CSV para análisis posterior. Incluye timestamp, configuración del test y todas las métricas medidas.

### 12. experimentos/exp-05-distribucion-leaders.md

Documento de experimento enfocado en observar y entender cómo Kafka distribuye automáticamente los leaders de particiones entre múltiples brokers.

**Objetivo:** Demostrar que Kafka distribuye leaders de particiones de forma balanceada entre todos los brokers disponibles en el clúster, maximizando el uso de recursos y evitando cuellos de botella.

**Hipótesis:** Si creamos un topic con múltiples particiones en un clúster de tres brokers, Kafka asignará aproximadamente un tercio de las particiones como leaders a cada broker.

**Conceptos previos necesarios:** Entender qué es un leader de partición. El leader es el broker responsable de todas las lecturas y escrituras de una partición específica. Solo puede haber un leader por partición en un momento dado. La distribución de leaders entre brokers es crítica para balancear la carga de trabajo.

**Pre-requisitos:**
- Clúster de tres brokers iniciado y verificado con el script veintiuno
- ClusterAnalyzer compilado y listo para ejecutar

**Procedimiento paso a paso:**

Paso uno: Iniciar el clúster ejecutando el script veinte-iniciar-cluster.ps1. Esperar a que los tres brokers estén completamente operativos. Verificar con docker ps que los tres contenedores estén corriendo.

Paso dos: Crear un topic con seis particiones y factor de replicación uno. Ejecuta este comando dentro del contenedor del broker uno:

```powershell
docker exec kafka-broker-1 kafka-topics --create `
  --topic transacciones-6p `
  --partitions 6 `
  --replication-factor 1 `
  --bootstrap-server localhost:9092
```

Explica por qué elegimos seis particiones específicamente. Seis es múltiplo de tres (número de brokers), lo cual facilita una distribución perfecta si Kafka balancea correctamente.

Paso tres: Inmediatamente después de crear el topic, ejecutar el script veintidós-describir-distribucion.ps1 para ver cómo Kafka asignó los leaders. Copia la salida completa a experimentos/resultados/distribucion-leaders.txt.

Paso cuatro: Ejecutar el ClusterAnalyzer desde Java para obtener una vista programática de la misma distribución:

```cmd
cd java
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-3-1.3.0.jar ^
  com.nexus.kafka.nivel3.ClusterAnalyzer transacciones-6p
```

Paso cinco: Crear un segundo topic con doce particiones para ver si el patrón de distribución se mantiene con más particiones:

```powershell
docker exec kafka-broker-1 kafka-topics --create `
  --topic transacciones-12p `
  --partitions 12 `
  --replication-factor 1 `
  --bootstrap-server localhost:9092
```

Paso seis: Analizar nuevamente la distribución de este nuevo topic. Verificar que cada broker tenga aproximadamente cuatro particiones como leader (un tercio de doce).

Paso siete: Crear un tercer topic con cinco particiones (un número que NO es múltiplo de tres) para ver cómo Kafka maneja la distribución cuando no puede ser perfectamente equitativa:

```powershell
docker exec kafka-broker-1 kafka-topics --create `
  --topic transacciones-5p `
  --partitions 5 `
  --replication-factor 1 `
  --bootstrap-server localhost:9092
```

Paso ocho: Analizar esta distribución y observar que probablemente dos brokers tendrán dos particiones y uno tendrá solo una partición. Documentar exactamente cómo se distribuyó.

**Resultados esperados:**

Para el topic de seis particiones, esperas ver una distribución perfecta donde cada broker es leader de exactamente dos particiones. La salida del script debería mostrar algo como broker uno con particiones cero y tres, broker dos con particiones uno y cuatro, broker tres con particiones dos y cinco.

Para el topic de doce particiones, cada broker debería ser leader de exactamente cuatro particiones, manteniendo el balance perfecto de un tercio cada uno.

Para el topic de cinco particiones, la distribución no puede ser perfecta. Esperarías ver dos brokers con dos particiones y un broker con una partición. Por ejemplo, broker uno con particiones cero y tres, broker dos con particiones uno y cuatro, broker tres solo con partición dos.

**Análisis:**

Kafka usa un algoritmo determinístico para asignar leaders de particiones. El algoritmo intenta distribuir los leaders de forma balanceada entre todos los brokers disponibles. Cuando el número de particiones es múltiplo del número de brokers, la distribución es perfecta. Cuando no lo es, Kafka hace su mejor esfuerzo para minimizar la diferencia entre el broker más cargado y el menos cargado.

Esta distribución de leaders es crítica para el rendimiento del clúster porque cada operación de lectura y escritura en una partición va al broker que es leader de esa partición. Si todos los leaders estuvieran en un solo broker, ese broker se convertiría en un cuello de botella mientras los otros brokers estarían subutilizados.

La distribución balanceada significa que cuando produces mensajes a este topic, el trabajo de procesar esas escrituras se distribuye automáticamente entre los tres brokers. Similarmente, cuando consumers leen del topic, están efectivamente leyendo de los tres brokers en paralelo.

**Conclusiones:**

Resume lo que observaste sobre la estrategia de distribución de Kafka. Confirma o refuta tu hipótesis inicial. Explica por qué esta distribución automática es valiosa en un entorno de producción donde podrías tener cientos de topics y miles de particiones.

### 13. experimentos/exp-06-carga-balanceada.md

Este experimento demuestra que cuando produces mensajes a un topic particionado en un clúster multi-broker, la carga de trabajo se distribuye automáticamente entre los brokers según dónde estén los leaders de las particiones.

**Objetivo:** Observar cómo el trabajo de procesar writes se distribuye entre múltiples brokers cuando produces mensajes a un topic particionado.

**Hipótesis:** Si produzco mensajes uniformemente distribuidos entre todas las particiones de un topic, cada broker procesará aproximadamente el mismo número de writes porque cada broker es leader de aproximadamente el mismo número de particiones.

**Pre-requisitos:**
- Clúster de tres brokers corriendo
- Topic transacciones-6p creado en experimento anterior
- LoadBalancedProducer compilado

**Procedimiento paso a paso:**

Paso uno: Antes de producir mensajes, ejecutar el script veinticuatro-ver-logs-brokers.ps1 para ver los logs iniciales de los tres brokers. Esto establece una línea base antes de generar carga.

Paso dos: En una ventana separada de PowerShell o CMD, iniciar el monitoreo de recursos de los contenedores:

```powershell
docker stats kafka-broker-1 kafka-broker-2 kafka-broker-3
```

Deja esta ventana abierta para observar el uso de CPU y red mientras produces mensajes.

Paso tres: Ejecutar el LoadBalancedProducer para enviar seiscientos mensajes con estrategia de distribución hash:

```cmd
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-3-1.3.0.jar ^
  com.nexus.kafka.nivel3.LoadBalancedProducer ^
  transacciones-6p 600 hash
```

Paso cuatro: Mientras el producer está enviando mensajes, observa la ventana de docker stats. Deberías ver actividad de CPU y red en los tres brokers simultáneamente, no solo en uno.

Paso cinco: Cuando el LoadBalancedProducer termine, examina su reporte de distribución. Verifica que cada broker procesó aproximadamente doscientos mensajes (un tercio del total).

Paso seis: Revisar los logs de los tres brokers nuevamente con el script veinticuatro. Busca líneas que mencionen "append" o "log" que indican escrituras al log de la partición. Verifica que hay actividad de escritura en los tres brokers.

Paso siete: Usar el PartitionAnalyzer del Nivel Dos para verificar que los mensajes efectivamente se escribieron en todas las particiones:

```cmd
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-2-1.2.0.jar ^
  com.nexus.kafka.nivel2.PartitionAnalyzer transacciones-6p
```

**Resultados esperados:**

El reporte del LoadBalancedProducer debería mostrar una distribución perfecta o casi perfecta. Aproximadamente cien mensajes por partición, lo que significa aproximadamente doscientos mensajes procesados por cada broker.

En la ventana de docker stats, durante el envío de mensajes deberías haber visto picos de CPU y tráfico de red en los tres brokers simultáneamente. Si solo hubieras visto actividad en un broker, eso indicaría un problema con la distribución.

**Análisis:**

El balanceo automático de carga es uno de los beneficios clave de usar múltiples brokers con topics particionados. Sin hacer nada especial en tu código de producer, Kafka automáticamente distribuyó el trabajo entre los tres servidores.

Compara esto con lo que pasaría si tuvieras un solo broker o un solo archivo de log. Todo el trabajo de procesar seiscientos writes sería responsabilidad de un solo servidor, limitando tu throughput al rendimiento de ese servidor individual.

Con tres brokers, cada uno solo procesó doscientos writes, un tercio de la carga total. Esto significa que puedes manejar tres veces más throughput antes de saturar tus servidores. Si necesitas aún más capacidad, podrías agregar más brokers y más particiones, escalando horizontalmente.

**Conclusiones:**

Resume cómo el particionamiento y la distribución de leaders entre múltiples brokers habilitan escalabilidad horizontal del throughput de writes en Kafka.

### 14. experimentos/exp-07-throughput-multibroker.md

Este experimento mide cuantitativamente la mejora de throughput que obtienes con un clúster multi-broker versus un solo broker.

**Objetivo:** Medir empíricamente la diferencia de throughput entre producir mensajes a un clúster de un broker versus un clúster de tres brokers.

**Hipótesis:** Un clúster de tres brokers puede procesar aproximadamente tres veces más mensajes por segundo que un solo broker porque el trabajo se distribuye entre tres servidores.

**Advertencia:** Este es un benchmark sintético en Docker en tu máquina local. Los números absolutos no son representativos de un clúster de producción en hardware dedicado, pero las proporciones relativas (la mejora de uno a tres brokers) sí deberían ser observables.

**Pre-requisitos:**
- Clúster de tres brokers corriendo
- ThroughputBenchmark compilado
- Topic transacciones-6p con seis particiones

**Procedimiento paso a paso:**

Paso uno: Ejecutar un benchmark con el clúster de tres brokers activo usando un solo thread productor:

```cmd
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-3-1.3.0.jar ^
  com.nexus.kafka.nivel3.ThroughputBenchmark ^
  --topic transacciones-6p ^
  --messages 10000 ^
  --message-size 1024 ^
  --threads 1 ^
  --batch-size 100 ^
  --linger-ms 10
```

Registra el throughput en mensajes por segundo que reporta el benchmark. Guarda este número como línea base.

Paso dos: Ejecutar el mismo benchmark pero con tres threads productores para maximizar el paralelismo:

```cmd
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-3-1.3.0.jar ^
  com.nexus.kafka.nivel3.ThroughputBenchmark ^
  --topic transacciones-6p ^
  --messages 10000 ^
  --message-size 1024 ^
  --threads 3 ^
  --batch-size 100 ^
  --linger-ms 10
```

Registra este throughput. Debería ser significativamente mayor que con un solo thread porque ahora estás aprovechando el paralelismo de los múltiples brokers.

Paso tres: Para comparación, necesitarías detener dos de los tres brokers y dejar solo uno corriendo. Sin embargo, esto es complicado con la configuración actual. En su lugar, crea un topic con una sola partición que efectivamente fuerza todo el trabajo a un solo broker:

```powershell
docker exec kafka-broker-1 kafka-topics --create `
  --topic transacciones-1p-benchmark `
  --partitions 1 `
  --replication-factor 1 `
  --bootstrap-server localhost:9092
```

Paso cuatro: Ejecutar el benchmark contra este topic de una partición con tres threads:

```cmd
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-3-1.3.0.jar ^
  com.nexus.kafka.nivel3.ThroughputBenchmark ^
  --topic transacciones-1p-benchmark ^
  --messages 10000 ^
  --message-size 1024 ^
  --threads 3 ^
  --batch-size 100 ^
  --linger-ms 10
```

Paso cinco: Comparar los throughputs. El topic de seis particiones debería mostrar throughput significativamente mayor que el topic de una partición, incluso con el mismo número de threads productores.

**Resultados esperados:**

Deberías observar que el topic de seis particiones (distribución entre tres brokers) logra mayor throughput que el topic de una partición (forzado a un solo broker). La mejora puede no ser exactamente tres veces debido a overhead y limitaciones del entorno Docker local, pero debería ser notable.

Por ejemplo, podrías ver algo como dos mil quinientos mensajes por segundo con el topic de seis particiones versus mil mensajes por segundo con el topic de una partición.

**Análisis:**

La diferencia de throughput demuestra que distribuir particiones entre múltiples brokers efectivamente paraleliza el trabajo de procesamiento. Cada broker puede escribir a sus particiones independientemente sin competir con los otros brokers por recursos.

Esta es la razón fundamental por la que Kafka escala horizontalmente. Cuando necesitas más throughput, agregas más brokers y más particiones, distribuyendo la carga entre más hardware.

En un entorno de producción con hardware dedicado y red rápida, las mejoras serían aún más dramáticas. Un clúster de diez brokers con cien particiones puede fácilmente procesar millones de mensajes por segundo.

**Conclusiones:**

Resume los resultados numéricos y explica cómo estos demuestran el valor de la arquitectura distribuida de Kafka.

### 15. INSTRUCCIONES-NIVEL-3.md

Documento maestro con instrucciones paso a paso para completar todo el Nivel Tres del laboratorio.

**Introducción:** Explica que en este nivel pasarás de un broker a tres brokers, verás distribución automática de particiones y medirás mejoras de throughput.

**Sección uno: Preparación del entorno**

Verificar que tienes los niveles anteriores completados. Asegurarte de que Docker Desktop tiene suficiente RAM asignada (al menos seis gigabytes). Cerrar el clúster del Nivel Uno si todavía está corriendo.

**Sección dos: Inicio del clúster multi-broker**

Ejecutar el script veinte-iniciar-cluster.ps1 o veinte-iniciar-cluster.bat según tu preferencia. Esperar pacientemente a que los tres brokers estén completamente operativos. Esto puede tomar un par de minutos.

Ejecutar el script veintiuno-verificar-cluster.ps1 para confirmar que los tres brokers están registrados en el clúster y se conocen entre sí.

**Sección tres: Compilación del código Java del Nivel Tres**

Navegar al directorio java y ejecutar Maven para compilar las nuevas clases:

```cmd
cd java
mvn clean package
```

Verificar que la compilación fue exitosa y que se generó el JAR kafka-lab-nivel-3-1.3.0.jar.

Recordatorio importante: todos los comandos java deben usar "%JAVA_HOME%\bin\java" con comillas para asegurar compatibilidad en Windows.

**Sección cuatro: Experimento cinco - Distribución de leaders**

Seguir el documento exp-05-distribucion-leaders.md paso a paso. Crear los tres topics con diferentes números de particiones. Usar el script de descripción y el ClusterAnalyzer para observar cómo se distribuyeron los leaders.

Capturar screenshots o copiar la salida a archivos de texto en experimentos/resultados/ para referencia futura.

**Sección cinco: Experimento seis - Carga balanceada**

Seguir exp-06-carga-balanceada.md. Ejecutar el LoadBalancedProducer mientras monitorizas los stats de Docker. Observar cómo los tres brokers comparten la carga de trabajo.

**Sección seis: Experimento siete - Benchmark de throughput**

Seguir exp-07-throughput-multibroker.md. Ejecutar los benchmarks con diferentes configuraciones. Comparar resultados entre topic particionado versus topic de una sola partición.

Guardar todos los resultados de benchmark en experimentos/resultados/metricas-throughput.txt con timestamps y configuraciones usadas.

**Sección siete: Exploración adicional**

Comandos útiles para experimentar por tu cuenta. Por ejemplo, crear topics con diferentes números de particiones y observar la distribución. Probar con diferentes estrategias de particionamiento en tus producers.

Sugerencias de qué más explorar, como monitorear el cambio del controlador del clúster si detienes y reinicias brokers individualmente.

**Sección ocho: Limpieza**

Cómo detener el clúster con el script veintitrés-detener-cluster.ps1. Recordar que esto detiene los brokers pero mantiene los datos en volúmenes Docker.

Si quieres eliminar todo y empezar completamente limpio, usar el script noventa y nueve-reset-laboratorio.ps1 del instructivo de reset.

**Sección nueve: Próximos pasos**

Adelanto del Nivel Cuatro que cubrirá réplicas y tolerancia a fallos. Explica brevemente que agregaremos replication-factor mayor a uno para que cada partición tenga múltiples copias, habilitando alta disponibilidad.

Menciona que en Nivel Cuatro simularemos fallos de brokers para ver cómo el clúster continúa operando sin pérdida de datos.

---

## Validaciones importantes

Todos los scripts PowerShell deben verificar que el directorio de trabajo es correcto antes de ejecutar comandos Docker. Verificar que JAVA_HOME está configurado antes de ejecutar comandos Java. Proporcionar mensajes de error claros y accionables cuando algo falla.

Los scripts batch deben tener la misma funcionalidad que los PowerShell pero adaptados a la sintaxis de CMD de Windows.

El código Java debe incluir comentarios explicativos en puntos clave, especialmente cuando se trabaja con AdminClient o metadatos del clúster que son conceptos más avanzados.

---

## Formato de output

Todos los archivos con encoding UTF-8. Scripts con line endings CRLF para Windows. Código Java indentado con cuatro espacios. Archivos YAML con dos espacios. Comentarios abundantes pero no excesivos. Mensajes al usuario en español. Logs técnicos pueden estar en inglés.

---

## Checklist de validación

Genera un archivo VALIDACION-NIVEL-3.md con este checklist:

- [ ] Clúster de tres brokers inicia correctamente
- [ ] Los tres brokers se conocen entre sí (verificado con script veintiuno)
- [ ] Topics se crean correctamente con particiones distribuidas
- [ ] ClusterAnalyzer muestra distribución de leaders correctamente
- [ ] LoadBalancedProducer distribuye mensajes entre brokers
- [ ] ThroughputBenchmark ejecuta sin errores
- [ ] Experimento cinco completado - entiendo distribución de leaders
- [ ] Experimento seis completado - observé balanceo de carga
- [ ] Experimento siete completado - medí mejora de throughput
- [ ] Puedo detener y reiniciar el clúster sin problemas
- [ ] Entiendo cómo múltiples brokers mejoran throughput y distribución

---

## Pregunta final

Antes de generar todos estos archivos, hay algún aspecto de esta especificación que necesites que clarifique o alguna funcionalidad adicional que quieras incluir en el Nivel Tres?