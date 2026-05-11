# LABORATORIO KAFKA - NIVEL 1: Fundamentos y Entorno Base
## Contexto del proyecto

Necesito que me ayudes a crear un laboratorio de aprendizaje completo para Apache Kafka 4.0+ usando la arquitectura KRaft (sin ZooKeeper). Este es el Nivel Uno de una serie progresiva de laboratorios, enfocado en establecer el entorno base funcional y validar que todo está configurado correctamente.

**Objetivo de aprendizaje:** Al completar este nivel, entenderé cómo levantar un clúster básico de Kafka usando Docker, cómo ejecutar comandos administrativos básicos y cómo producir y consumir mi primer mensaje.

**Entorno de trabajo:** Windows 10, usando Docker Desktop. Necesito scripts tanto en PowerShell como en CMD para máxima compatibilidad.

**Versión objetivo:** Apache Kafka 4.0+ con KRaft (sin ZooKeeper).

---

## Requisitos previos que debo verificar antes de comenzar

Por favor genera un archivo de verificación que compruebe estos requisitos:

- Docker Desktop instalado y corriendo en Windows 10
- Puerto 9092 disponible (no usado por otra aplicación)
- Al menos 4GB de RAM disponible para Docker
- WSL2 habilitado (si Docker Desktop lo requiere)

---

## Estructura de directorios completa a crear

Genera esta estructura exacta en el directorio donde ejecute tus scripts:

kafka-lab-nivel-1/
├── docker/
│   ├── docker-compose.yml
│   └── kafka-config/
│       └── server.properties
├── scripts/
│   ├── powershell/
│   │   ├── 00-verificar-requisitos.ps1
│   │   ├── 01-iniciar-kafka.ps1
│   │   ├── 02-verificar-cluster.ps1
│   │   ├── 03-detener-kafka.ps1
│   │   └── 04-limpiar-todo.ps1
│   └── cmd/
│       ├── 00-verificar-requisitos.bat
│       ├── 01-iniciar-kafka.bat
│       ├── 02-verificar-cluster.bat
│       ├── 03-detener-kafka.bat
│       └── 04-limpiar-todo.bat
├── experimentos/
│   ├── exp-01-primer-mensaje.md
│   └── resultados/
│       └── .gitkeep
├── java/
│   ├── pom.xml
│   └── src/
│       └── main/
│           └── java/
│               └── com/
│                   └── nexus/
│                       └── kafka/
│                           └── nivel1/
│                               ├── SimpleProducer.java
│                               └── SimpleConsumer.java
├── logs/
│   └── .gitkeep
├── README.md
└── INSTRUCCIONES-NIVEL-1.md

---

## Archivos a generar con contenido completo

### 1. docker/docker-compose.yml

Genera un archivo docker-compose.yml para Kafka 4.0+ con KRaft que incluya:

- Un solo broker de Kafka configurado en modo KRaft (sin ZooKeeper)
- Variable KAFKA_NODE_ID configurada correctamente
- Configuración de listeners para acceso desde Windows (localhost:9092)
- Volúmenes persistentes mapeados correctamente para Windows
- Variables de entorno necesarias para KRaft: KAFKA_PROCESS_ROLES, KAFKA_CONTROLLER_QUORUM_VOTERS, etc.
- Configuración de KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 (apropiado para un solo broker)
- Health check para verificar que Kafka está listo
- Logs configurados para nivel INFO con rotación

**IMPORTANTE:** Usa la imagen oficial de Apache Kafka 4.0+ o Confluent Platform 7.7+ que soporte KRaft. Incluye comentarios explicando cada variable de configuración importante.

### 2. scripts/powershell/00-verificar-requisitos.ps1

Genera un script PowerShell que verifique:

- Si Docker Desktop está instalado (verificar que el comando `docker` existe)
- Si Docker Desktop está corriendo (intentar ejecutar `docker ps`)
- Si el puerto 9092 está disponible (verificar con `Test-NetConnection`)
- Si WSL2 está habilitado (si Docker lo requiere)
- Memoria disponible para Docker
- Versión de Docker instalada

El script debe imprimir mensajes claros con colores (verde para OK, rojo para ERROR, amarillo para WARNING) usando `Write-Host`. Al final debe mostrar un resumen de si el sistema está listo o qué falta corregir.

### 3. scripts/powershell/01-iniciar-kafka.ps1

Genera un script que:

- Verifique que estamos en el directorio correcto (debe existir docker/docker-compose.yml)
- Ejecute `docker-compose up -d` desde el directorio docker/
- Espere 30 segundos dando feedback visual del progreso
- Verifique que el contenedor está corriendo con `docker ps`
- Muestre los últimos logs del contenedor
- Intente una conexión de prueba al puerto 9092
- Imprima mensaje de éxito con instrucciones de qué hacer después

### 4. scripts/powershell/02-verificar-cluster.ps1

Genera un script que ejecute comandos de verificación dentro del contenedor de Kafka:

- Listar todos los brokers del clúster usando kafka-broker-api-versions
- Mostrar la configuración del broker
- Listar topics existentes (debería estar vacío inicialmente)
- Mostrar información del clúster usando kafka-metadata
- Todo debe ejecutarse dentro del contenedor con `docker exec`

### 5. scripts/powershell/03-detener-kafka.ps1

Script para detener el clúster de forma limpia:

- Ejecutar `docker-compose down` desde docker/
- Verificar que los contenedores se detuvieron
- Mostrar mensaje confirmando que se detuvo pero los datos persisten en volúmenes

### 6. scripts/powershell/04-limpiar-todo.ps1

Script para resetear completamente el laboratorio:

- Advertencia clara de que esto ELIMINARÁ todos los datos
- Pedir confirmación al usuario (Y/N)
- Si confirma: ejecutar `docker-compose down -v` para eliminar volúmenes
- Eliminar directorio logs/ y recrearlo
- Mostrar mensaje de que el entorno está limpio

### 7. scripts/cmd/ (versiones CMD de todos los anteriores)

Genera versiones equivalentes de todos los scripts PowerShell pero en formato .bat para CMD. Deben tener la misma funcionalidad pero usando sintaxis de batch. Usa `echo` con colores cuando sea posible.

### 8. java/pom.xml

Genera un archivo Maven con:

- groupId: com.nexus.kafka
- artifactId: kafka-lab-nivel-1
- version: 1.0.0
- Java 17 o superior
- Dependencia de Kafka clients versión 4.0+ (o la última compatible)
- Plugin maven-compiler configurado
- Plugin maven-shade para crear JAR ejecutable

### 9. java/src/.../SimpleProducer.java

Genera un producer simple pero altamente instrumentado que:

- Se conecte a localhost:9092
- Acepte dos parámetros: topic name y mensaje
- Use StringSerializer para key y value
- Configure acks=all para máxima durabilidad
- Imprima ANTES de enviar: "Enviando mensaje al topic [topic]: [mensaje]"
- Imprima DESPUÉS de enviar: "✅ Mensaje enviado exitosamente - Topic: [topic], Partition: [num], Offset: [offset], Timestamp: [timestamp]"
- Incluya manejo de errores con mensajes claros
- Incluya comentarios explicando cada configuración importante
- Use try-with-resources para cerrar recursos correctamente

### 10. java/src/.../SimpleConsumer.java

Genera un consumer simple pero instrumentado que:

- Se conecte a localhost:9092
- Acepte dos parámetros: topic name y group id
- Use StringDeserializer
- Configure auto.offset.reset=earliest para leer desde el principio
- Imprima cuando se suscriba: "🔵 Consumer iniciado - Topic: [topic], Group: [groupId]"
- Para cada mensaje recibido imprima: "📨 Mensaje recibido - Partition: [num], Offset: [offset], Key: [key], Value: [value], Timestamp: [timestamp]"
- Use poll con timeout de 1000ms en un loop infinito
- Incluya shutdown hook para cerrar limpiamente con Ctrl+C
- Incluya comentarios explicativos

### 11. experimentos/exp-01-primer-mensaje.md

Genera un documento de experimento estructurado con estas secciones:

**Título:** Experimento 01 - Producir y Consumir Mi Primer Mensaje

**Objetivo:** Validar que el clúster de Kafka está funcional enviando y recibiendo un mensaje simple.

**Hipótesis:** Si el clúster está configurado correctamente, podré enviar un mensaje con el producer y recibirlo inmediatamente con el consumer.

**Pre-requisitos:**
- Clúster Kafka iniciado y validado
- Código Java compilado

**Procedimiento paso a paso:**

Paso uno: Crear el topic manualmente usando kafka-topics desde dentro del contenedor. Especifica el comando exacto con explicación de cada parámetro (nombre del topic: "primer-topic", particiones: 1, replication-factor: 1).

Paso dos: Verificar que el topic se creó correctamente listando todos los topics.

Paso tres: Compilar el código Java con Maven. Proporciona el comando exacto.

Paso cuatro: Abrir dos terminales de PowerShell o CMD.

Paso cinco: En la primera terminal, ejecutar el SimpleConsumer conectándose a "primer-topic" con group id "grupo-prueba". Proporciona el comando java exacto.

Paso seis: En la segunda terminal, ejecutar el SimpleProducer enviando el mensaje "Hola Kafka desde Nivel 1" al topic "primer-topic". Proporciona el comando java exacto.

Paso siete: Observar el output en ambas terminales.

**Resultados esperados:**

Describe exactamente qué deberías ver en la terminal del consumer (el mensaje recibido con todos sus metadatos) y en la terminal del producer (confirmación de envío con partition y offset).

**Análisis:**

Explica por qué el mensaje llegó del producer al consumer, qué rol jugó el broker y qué significa cada campo de metadata que viste (partition, offset, timestamp).

**Conclusiones:**

Resume qué validamos con este experimento y qué aprendimos sobre el flujo básico de mensajes en Kafka.

### 12. README.md

Genera un README principal del proyecto que incluya:

- Título del proyecto: "Laboratorio Kafka - Nivel 1: Fundamentos"
- Descripción breve de qué es este laboratorio
- Requisitos del sistema
- Estructura del proyecto explicada
- Instrucciones rápidas de inicio (quick start)
- Enlaces a documentación relevante de Kafka
- Información de troubleshooting común en Windows

### 13. INSTRUCCIONES-NIVEL-1.md

Genera un documento de instrucciones detalladas paso a paso para completar el Nivel Uno:

**Sección 1: Preparación del entorno**
Instrucciones para verificar requisitos y preparar el workspace.

**Sección 2: Inicio del clúster**
Cómo ejecutar los scripts de inicio y qué verificar.

**Sección 3: Verificación del clúster**
Cómo ejecutar comandos de verificación y qué output esperar.

**Sección 4: Compilación del código Java**
Comandos Maven y verificación de que compiló correctamente.

**Sección 5: Ejecución del experimento 01**
Guía completa paso a paso siguiendo exp-01-primer-mensaje.md.

**Sección 6: Exploración adicional**
Comandos útiles para explorar el clúster (listar topics, describir topics, ver configuración del broker).

**Sección 7: Limpieza**
Cómo detener el clúster y opcionalmente limpiar todo.

**Sección 8: Próximos pasos**
Qué se cubrirá en el Nivel 2 (adelanto de particiones).

---

## Validaciones que debes incluir

Genera validaciones automáticas en los scripts donde sea apropiado:

- Verificar que comandos existen antes de ejecutarlos
- Verificar códigos de retorno de comandos Docker
- Timeout en operaciones que pueden colgar
- Mensajes de error claros cuando algo falla
- Sugerencias de solución para errores comunes en Windows

---

## Formato de output esperado

Por favor genera todos estos archivos con:

- Codificación UTF-8
- Line endings LF (Unix) para archivos de configuración
- Line endings CRLF (Windows) para scripts .ps1 y .bat
- Indentación consistente (2 espacios para YAML, 4 para Java)
- Comentarios abundantes pero no excesivos
- Nombres de variables descriptivos
- Mensajes de usuario en español
- Logs técnicos pueden estar en inglés

---

## Consideraciones especiales para Windows 10

Asegúrate de que:

- Las rutas usen backslash cuando sea necesario en scripts Windows
- Los volúmenes de Docker estén mapeados correctamente para Windows
- Los scripts verifiquen permisos de ejecución cuando sea relevante
- Se use `docker-compose` (con guion) no `docker compose` para compatibilidad
- Los comandos funcionen tanto en PowerShell 5.1 como 7+
- Los scripts .bat funcionen en CMD tradicional de Windows

---

## Después de generar todo

Una vez que hayas generado todos los archivos, por favor crea también un archivo VALIDACION-NIVEL-1.md que contenga un checklist que yo pueda marcar a medida que valido cada componente:

- [ ] Estructura de directorios creada correctamente
- [ ] Docker compose se levanta sin errores
- [ ] Scripts PowerShell se ejecutan sin errores
- [ ] Scripts CMD se ejecutan sin errores
- [ ] Código Java compila correctamente
- [ ] Experimento 01 completado exitosamente
- [ ] Puedo detener y reiniciar el clúster sin perder datos
- [ ] Entiendo cada componente del laboratorio

---

## Pregunta final antes de generar

¿Hay algún aspecto de esta especificación que no esté claro o que necesites que clarifique antes de comenzar a generar todos los archivos?