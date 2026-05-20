# Apache Kafka: Conceptos Básicos para Desarrolladores

**Audiencia:** Desarrolladores backend, fullstack y data engineers  
**Propósito:** Documento fuente para NotebookLM — presentación en 3 fases  
**Versión de referencia:** Apache Kafka 4.x (KRaft, sin ZooKeeper)

---

## FASE 1 — ¿Qué es y por qué existe Apache Kafka?

### El problema que Kafka resuelve

En la mayoría de las arquitecturas tradicionales, los sistemas se comunican de forma síncrona y directa: el Servicio A llama al Servicio B, espera su respuesta, y continúa. Este modelo funciona bien cuando la escala es pequeña, pero se vuelve frágil a medida que el sistema crece.

Cuando hay diez o veinte servicios comunicándose entre sí de forma directa, surgen problemas críticos:

- **Acoplamiento fuerte:** si un servicio falla o responde lento, todos los que dependen de él se ven afectados directamente.
- **Escalabilidad costosa:** escalar un servicio implica escalar todos los servicios que dependen de él.
- **Pérdida de datos:** si el servicio receptor no está disponible en el momento exacto de la llamada, el mensaje o evento se pierde.
- **Falta de trazabilidad:** reconstruir qué pasó y cuándo requiere revisar logs de múltiples sistemas por separado.

Un ejemplo concreto: una tienda en línea tiene un servicio de pedidos que, al recibir un pedido, llama directamente a tres servicios: inventario, notificaciones y facturación. Si el servicio de notificaciones tarda 30 segundos en responder, el usuario espera 30 segundos aunque inventario y facturación respondan en milisegundos. Si notificaciones se cae durante el Black Friday, los pedidos no se pueden completar aunque todo lo demás esté funcionando perfectamente.

Este es el problema que motivó la creación de Apache Kafka.

---

### La evolución de la mensajería hacia el streaming

Los sistemas de mensajería existen desde hace décadas para resolver el problema del acoplamiento entre servicios. En los años noventa aparecieron los primeros message brokers como IBM MQ y ActiveMQ, que permitían que los servicios se comunicaran a través de colas en lugar de llamadas directas.

Sin embargo, estos sistemas tenían una limitación fundamental: cuando un consumer leía un mensaje de la cola, ese mensaje desaparecía. Si otro servicio necesitaba el mismo evento, había que duplicar la cola o la lógica de distribución.

En los años 2000 surgieron los ESB (Enterprise Service Bus) como intentos de centralizar la integración, pero se convirtieron en monolitos de integración difíciles de mantener y escalar.

En 2011, un equipo de LinkedIn liderado por Jay Kreps, Neha Narkhede y Jun Rao propuso una idea diferente: en lugar de una cola que destruye el mensaje al leerlo, ¿qué tal un **log distribuido y persistente** donde los mensajes se quedan escritos y cualquier sistema puede leerlos a su propio ritmo, incluyendo eventos del pasado?

Esa idea se convirtió en Apache Kafka.

---

### ¿Qué es Apache Kafka?

Apache Kafka es un **sistema de log distribuido, particionado y replicado** diseñado para manejar flujos de eventos en tiempo real a altísima escala.

La definición más precisa es: *"una plataforma de streaming de eventos a la que muchos productores pueden escribir y de la que muchos consumidores pueden leer de forma completamente independiente, a su propio ritmo, incluyendo eventos que ocurrieron en el pasado."*

Lo que distingue a Kafka de una cola de mensajes tradicional son cuatro características fundamentales:

**1. Alta disponibilidad:** Kafka replica los datos en múltiples servidores. Si uno falla, el sistema continúa operando sin pérdida de datos ni interrupción del servicio.

**2. Alto rendimiento:** Kafka está diseñado para manejar millones de eventos por segundo con latencias de milisegundos. Empresas como LinkedIn procesan más de un billón de eventos diarios con Kafka.

**3. Persistencia configurable:** los mensajes no desaparecen al ser leídos. Se mantienen almacenados durante el tiempo que se configure: horas, días, semanas, o indefinidamente.

**4. Replay de eventos:** cualquier consumer puede volver a leer eventos del pasado desde cualquier punto del historial. Esto permite reprocesar datos, corregir errores o incorporar nuevos servicios que necesiten el historial completo.

Kafka fue donado a la Apache Software Foundation en 2011 y desde entonces ha crecido hasta convertirse en la plataforma de streaming de eventos más utilizada en la industria. Más del 80% de las empresas del Fortune 100 lo usan en producción.

---

### Historia y evolución de Apache Kafka

**2010:** Jay Kreps, Neha Narkhede y Jun Rao desarrollan Kafka internamente en LinkedIn para resolver el problema de mover cientos de millones de eventos diarios entre los distintos sistemas de la plataforma.

**2011:** LinkedIn dona Kafka a la Apache Software Foundation. El proyecto se convierte en código abierto y comienza a adoptarse en la industria.

**2014:** Los creadores originales de Kafka fundan Confluent, empresa dedicada a construir el ecosistema comercial alrededor de Kafka.

**2017:** Maduran Kafka Streams (procesamiento de streams directamente en la JVM) y Kafka Connect (integración con sistemas externos), completando el ecosistema core.

**2022:** KRaft llega a producción. Este nuevo mecanismo de consenso interno elimina la dependencia de ZooKeeper, simplificando radicalmente la operación y la arquitectura del clúster.

**2024:** Kafka 4.x se lanza como la primera versión que es exclusivamente KRaft. ZooKeeper ya no es una opción. La arquitectura es más simple, más rápida y soporta órdenes de magnitud más particiones por clúster.

---

## FASE 2 — Conceptos Core de Apache Kafka

### Arquitectura general

La arquitectura de Kafka se puede describir con tres componentes principales:

**Producers** (productores): son los sistemas o aplicaciones que generan eventos y los publican en Kafka. Un servicio de pedidos, una aplicación web, un sensor IoT, o cualquier sistema que genere datos puede ser un producer.

**Kafka Cluster** (clúster): es el corazón del sistema. Está formado por uno o más servidores llamados brokers que almacenan los eventos de forma distribuida y los sirven a los consumers. El clúster gestiona la replicación, la tolerancia a fallos y la distribución de la carga.

**Consumers** (consumidores): son los sistemas que leen eventos de Kafka. Un servicio de notificaciones, un pipeline de datos, un sistema de analytics, o cualquier aplicación que necesite procesar eventos puede ser un consumer.

La dirección del flujo es siempre: Producers → Kafka Cluster → Consumers. Los producers y consumers están completamente desacoplados: no se conocen entre sí, no se llaman directamente, y operan de forma independiente.

---

### El log distribuido: la idea central

El concepto fundamental detrás de Kafka es el **log distribuido**. Un log es una secuencia ordenada de registros donde cada nuevo registro se añade al final. Una vez escrito, un registro nunca se modifica ni se borra durante el período de retención configurado.

Esta estructura tiene propiedades muy poderosas:

- **Inmutabilidad:** los eventos no se alteran. El historial es confiable.
- **Orden garantizado:** los eventos dentro de una partición siempre están en el orden en que llegaron.
- **Lectura independiente:** múltiples sistemas pueden leer el mismo log en paralelo sin afectarse entre sí. Cada uno mantiene su propia posición de lectura.
- **Replay:** cualquier consumer puede volver a una posición anterior del log y releer eventos históricos.

Esta última propiedad es transformadora. En un sistema tradicional de colas, si se introduce un nuevo servicio que necesita el historial de eventos, no hay forma de obtenerlo — los mensajes ya fueron consumidos y destruidos. En Kafka, un nuevo servicio puede leer desde el inicio del log y ponerse al día con todo el historial disponible.

---

### Topics

Un **topic** es la unidad lógica de organización en Kafka. Es un canal con nombre al que los producers publican eventos y del que los consumers leen.

Cada topic tiene un nombre único en el clúster. La convención más común en la industria es usar nombres descriptivos en formato kebab-case que indiquen el dominio y el tipo de evento: `order-created`, `payment-processed`, `user-registered`, `inventory-updated`.

Una arquitectura típica tiene múltiples topics, cada uno representando un tipo diferente de evento de negocio. El servicio de pedidos publica en `order-created`. El servicio de pagos publica en `payment-processed`. Cada consumidor suscribe solo a los topics que necesita.

Los topics son configurables en varios aspectos: número de particiones (que determina el paralelismo), factor de replicación (que determina la durabilidad), y política de retención (cuánto tiempo o cuánto espacio se mantienen los mensajes).

---

### Particiones

Cada topic se divide en una o más **particiones**. Las particiones son el mecanismo por el cual Kafka logra escalar horizontalmente y procesar datos en paralelo.

Físicamente, cada partición es un log independiente: una secuencia ordenada de mensajes almacenada en uno de los brokers del clúster. Un topic con 6 particiones puede distribuir esas particiones entre varios brokers, permitiendo que múltiples productores escriban en paralelo y múltiples consumidores lean en paralelo.

La propiedad más importante de las particiones respecto al orden es que **el orden de los mensajes solo se garantiza dentro de una partición, no entre particiones**. Si el orden es crítico para un conjunto de mensajes (por ejemplo, todos los eventos de un mismo pedido), hay que asegurar que esos mensajes vayan siempre a la misma partición. Esto se logra usando una clave de particionado.

Cuando un producer envía un mensaje con una clave, Kafka garantiza que todos los mensajes con la misma clave siempre irán a la misma partición. Si se usa el ID del cliente como clave, todos los eventos de un mismo cliente siempre estarán en la misma partición y en orden.

---

### Offsets

Dentro de cada partición, cada mensaje tiene asignado un **offset**: un número entero secuencial que identifica de forma única la posición de ese mensaje dentro de la partición. El primer mensaje tiene offset 0, el segundo offset 1, y así sucesivamente.

Los offsets son la forma en que Kafka permite que los consumers mantengan su posición de lectura de forma independiente. Cada consumer sabe en qué offset está y puede avanzar, retroceder o saltar a cualquier posición según lo necesite.

Existen varios tipos de offset relevantes para el monitoreo y operación:

- **Log-end offset:** la posición del último mensaje escrito en la partición. Indica el total de mensajes producidos.
- **Current offset:** la posición actual del consumer, el próximo mensaje que leerá.
- **Committed offset:** la última posición que el consumer ha confirmado como procesada exitosamente.
- **Consumer lag:** la diferencia entre el log-end offset y el committed offset. Indica cuántos mensajes están pendientes de procesar.

El consumer lag es una de las métricas más importantes en operación de Kafka. Un lag creciente indica que el consumer no puede procesar eventos a la misma velocidad que el producer los genera, lo que requiere atención: escalar los consumers, optimizar el procesamiento, o incrementar las particiones.

---

### Producers

Un **producer** es cualquier aplicación que publica mensajes en un topic de Kafka.

El flujo de publicación de un producer es el siguiente: el producer serializa el mensaje (convierte el objeto a bytes usando un serializador configurado, como JSON, Avro o Protobuf), determina a qué partición enviar el mensaje (por clave, por round-robin, o usando un particionador personalizado), envía el mensaje al broker que es líder para esa partición, y espera la confirmación de escritura según la configuración de durabilidad.

La configuración más crítica de un producer es **acks** (acknowledgements), que determina cuándo Kafka considera que un mensaje fue escrito exitosamente:

- `acks=0`: el producer no espera confirmación. Máximo rendimiento, pero puede perder mensajes si el broker falla.
- `acks=1`: el broker líder confirma que escribió el mensaje. Balance entre rendimiento y durabilidad.
- `acks=all`: todos los brokers que tienen réplicas de esa partición confirman la escritura. Máxima durabilidad, nunca se pierde un mensaje confirmado.

Para producción crítica, la combinación recomendada es `acks=all`, `retries` mayor a cero, y `enable.idempotence=true`, que garantiza que incluso si el producer reintenta un mensaje, no se duplicará en el topic.

---

### Consumers

Un **consumer** es cualquier aplicación que lee mensajes de un topic de Kafka.

El modelo de lectura de Kafka es **pull**: el consumer solicita mensajes al broker a su propio ritmo. El broker no empuja mensajes al consumer. Esta decisión de diseño es fundamental porque significa que el consumer controla su velocidad de procesamiento de forma natural. Si el consumer está ocupado procesando, simplemente no hace la siguiente solicitud. No hay riesgo de que el broker lo sature con más mensajes de los que puede manejar.

Cuando un consumer lee un mensaje y lo procesa exitosamente, **hace commit del offset**: le indica a Kafka que procesó hasta esa posición. Si el consumer falla antes de hacer commit y se reinicia, retomará desde el último offset commitado, garantizando que ningún mensaje se pierda.

Dos configuraciones clave para el comportamiento inicial de un consumer son:
- `auto.offset.reset=earliest`: si el consumer no tiene un offset previo registrado, empieza desde el inicio del topic (lee todo el historial disponible).
- `auto.offset.reset=latest`: si el consumer no tiene un offset previo, empieza desde el mensaje más reciente (ignora el historial).

---

### Consumer Groups

Un **consumer group** es un conjunto de consumers que colaboran para leer y procesar un topic en paralelo, dividiéndose las particiones entre ellos.

El mecanismo funciona así: Kafka asigna cada partición del topic a exactamente un consumer del grupo. Si el topic tiene 6 particiones y el grupo tiene 3 consumers, cada consumer procesa 2 particiones. Si el grupo tiene 6 consumers, cada uno procesa 1 partición. Si el grupo tiene más consumers que particiones, los consumers sobrantes quedan en standby listos para tomar el relevo si uno de los activos falla.

Esta asignación garantiza que cada mensaje sea procesado exactamente una vez dentro del grupo, lo que es el comportamiento deseado para la mayoría de los casos de uso.

El consumer group tiene un **Group ID** que lo identifica en el clúster. Dos grupos con distinto ID son completamente independientes: ambos recibirán todos los mensajes del topic. Esto permite que un mismo topic sea consumido simultáneamente por un servicio de notificaciones, un servicio de analytics y un pipeline de datos, sin que unos afecten a los otros.

Cuando un consumer del grupo falla, Kafka detecta la ausencia y ejecuta un **rebalanceo**: reasigna las particiones que estaban asignadas al consumer caído entre los consumers restantes del grupo. El proceso es automático y garantiza que el procesamiento continúe sin intervención manual.

---

### Brokers y Clúster

Un **broker** es un servidor de Kafka. Su responsabilidad es almacenar las particiones de los topics, recibir escrituras de los producers, servir lecturas a los consumers y mantener las réplicas sincronizadas con los otros brokers.

Un **clúster de Kafka** es un conjunto de brokers que trabajan juntos para distribuir la carga y garantizar la disponibilidad del sistema. La práctica estándar en producción es tener al menos 3 brokers, lo que permite tolerar la pérdida de 1 broker sin interrumpir el servicio.

Cada partición tiene un **broker líder**: el broker que recibe todas las escrituras y lecturas para esa partición. El resto de los brokers que tienen réplicas de esa partición actúan como **followers**: copian continuamente los datos del líder para mantenerse sincronizados.

Si el broker líder de una partición falla, el clúster detecta el problema y **elige automáticamente un nuevo líder** entre los followers sincronizados. Este proceso ocurre en segundos y es invisible para los producers y consumers, que simplemente empiezan a comunicarse con el nuevo líder.

El **factor de replicación** determina cuántas copias existen de cada partición. Con factor de replicación 3 (el estándar de la industria), cada partición tiene 1 líder y 2 followers distribuidos en brokers distintos. Esto garantiza que se pueden perder 2 de los 3 brokers sin perder datos.

---

### KRaft: Kafka sin ZooKeeper

Hasta Kafka 3.x, el clúster dependía de **Apache ZooKeeper** para coordinar las decisiones de consenso: qué broker es el controller, quién es el líder de cada partición, qué consumers pertenecen a qué grupos. Esto significaba que para operar Kafka en producción había que operar también un clúster de ZooKeeper por separado — con su propia infraestructura, monitoreo y operación.

Desde Kafka 4.x, ZooKeeper fue eliminado completamente. El reemplazo es **KRaft** (Kafka Raft Metadata Protocol): un mecanismo de consenso distribuido basado en el algoritmo Raft, embebido directamente en los propios brokers de Kafka.

Con KRaft, el consenso del clúster — quién es el controller, cuáles son los líderes de partición, cuál es el estado de los consumer groups — se gestiona internamente entre los propios brokers, sin dependencias externas.

Los beneficios de KRaft son concretos:

- **Operación simplificada:** un solo sistema que operar y monitorear en lugar de dos (Kafka + ZooKeeper).
- **Arranque más rápido:** los brokers arrancan y se unen al clúster en segundos en lugar de decenas de segundos.
- **Escalabilidad masiva:** KRaft soporta millones de particiones por clúster, frente a las decenas de miles que era el límite práctico con ZooKeeper.
- **Recuperación más rápida:** el tiempo de elección de nuevo líder tras un fallo es mucho menor.

Para desarrolladores, el efecto inmediato es que iniciar Kafka localmente es trivial: un solo contenedor Docker con la imagen `apache/kafka:4.2.0` es suficiente para tener un broker funcional en segundos, sin configuración adicional.

---

## FASE 3 — Kafka en Acción

### Casos de uso reales en la industria

Apache Kafka se utiliza en producción en una amplia variedad de casos de uso. Los más comunes en la industria son:

**Event Sourcing en microservicios:** en lugar de que los microservicios se llamen directamente entre sí, cada servicio publica eventos en Kafka cuando ocurre algo importante (pedido creado, pago procesado, usuario registrado). Los demás servicios subscriben a los eventos que les interesan y reaccionan de forma asíncrona. Esto elimina el acoplamiento directo y hace el sistema más resiliente. Empresas como Uber y Airbnb usan este patrón para coordinar decenas o cientos de microservicios.

**Pipelines de datos en tiempo real:** Kafka actúa como la columna vertebral que conecta sistemas fuente (bases de datos, aplicaciones) con sistemas destino (data lakes, data warehouses, motores de búsqueda). Los datos fluyen continuamente y están disponibles para análisis en tiempo real o casi real. LinkedIn usa Kafka para mover datos entre sus múltiples sistemas de almacenamiento y análisis.

**Métricas, logs y telemetría:** en sistemas a gran escala con miles de servidores o microservicios, centralizar logs y métricas es un reto de ingestión masiva de datos. Kafka maneja la ingestión de millones de eventos de log por segundo y los distribuye a sistemas de almacenamiento y análisis como Elasticsearch, Splunk o sistemas propietarios. Netflix utiliza Kafka para centralizar toda la telemetría de su infraestructura global.

**Detección de fraude en tiempo real:** el análisis de patrones de comportamiento en transacciones financieras requiere procesar cada transacción en milisegundos y correlacionarla con el historial reciente. Kafka permite ingestar las transacciones en tiempo real y alimentar modelos de detección de fraude que emiten alertas en tiempo real. Empresas como Visa y PayPal procesan sus flujos de transacciones con Kafka.

**Procesamiento de datos IoT:** dispositivos IoT — sensores industriales, vehículos conectados, dispositivos de salud — generan flujos continuos de datos a muy alto volumen. Kafka es ideal para ingestar estos flujos desde miles o millones de dispositivos simultáneamente y distribuirlos a los sistemas de procesamiento y almacenamiento correspondientes.

**Change Data Capture (CDC):** Kafka, combinado con herramientas como Debezium, permite capturar cada cambio que ocurre en una base de datos relacional (inserciones, actualizaciones, eliminaciones) y publicarlos como eventos en Kafka. Esto permite mantener múltiples sistemas sincronizados con la base de datos fuente en tiempo real, sin necesidad de polling ni ETL batch.

---

### Kafka vs otras herramientas de mensajería

Kafka no es la herramienta adecuada para todos los casos de mensajería. Es importante entender cuándo Kafka es la elección correcta y cuándo otras herramientas son más apropiadas.

**Apache Kafka** es la elección correcta cuando se necesita alto rendimiento (millones de mensajes por segundo), retención de mensajes para replay o auditoría, múltiples consumers independientes que consuman el mismo flujo de eventos, o procesamiento de streams de datos en tiempo real. La complejidad operacional de Kafka se justifica cuando el problema tiene una escala o unos requisitos que las alternativas no pueden satisfacer.

**RabbitMQ** es más adecuado cuando el caso de uso es una cola de trabajo clásica: una tarea se pone en la cola, un worker la toma, la procesa y el mensaje desaparece. RabbitMQ tiene menor complejidad operacional que Kafka y su modelo de push es más natural para workflows con respuestas. No ofrece replay ni escala de Kafka, pero para volúmenes de miles de mensajes por segundo es una solución más simple.

**Amazon SQS** es la opción más sencilla para colas en infraestructura AWS. Es un servicio completamente gestionado sin infraestructura que operar, con un modelo de precios por uso. No ofrece retención larga ni replay, y el máximo de retención son 14 días. Ideal para desacoplar componentes en arquitecturas cloud sin la complejidad de Kafka.

**Redis Pub/Sub** ofrece mensajería en tiempo real con latencias extremadamente bajas, pero sin durabilidad: los mensajes no se persisten, si un consumer no está conectado en el momento en que llega el mensaje, lo pierde. Útil para notificaciones en tiempo real, chat, o invalidación de caché, pero no para eventos de negocio críticos.

El criterio de decisión más simple: si necesitas que los mensajes persistan y que múltiples sistemas independientes los consuman — incluyendo eventos del pasado — Kafka es la herramienta. Si solo necesitas desacoplar dos servicios con un volumen manejable y sin requisitos de replay, las alternativas más simples son suficientes.

---

### El ecosistema de Apache Kafka

Apache Kafka tiene un ecosistema de componentes que extienden sus capacidades más allá del core de mensajería.

**Kafka Connect** es el framework de integración de Kafka. Permite conectar Kafka con sistemas externos — bases de datos, sistemas de archivos, servicios cloud, motores de búsqueda — usando conectores preexistentes sin escribir código. Existen más de 200 conectores disponibles en la comunidad. Los **source connectors** leen datos de un sistema externo y los publican en Kafka. Los **sink connectors** leen datos de Kafka y los escriben en un sistema externo. Kafka Connect maneja la escalabilidad, la tolerancia a fallos y el monitoreo de estas integraciones de forma automática.

**Kafka Streams** es una librería Java para procesar streams de eventos directamente en la JVM, sin necesidad de un sistema de procesamiento externo. Permite realizar operaciones como filtrado, transformación, agregaciones por ventanas de tiempo, y joins entre streams de forma declarativa. Una aplicación de Kafka Streams es simplemente una aplicación Java normal que escala añadiendo más instancias.

**Apache Flink** es el sistema de procesamiento de streams más usado junto con Kafka para casos que requieren procesamiento más complejo: joins de streams con estado, gestión sofisticada del tiempo, o procesamiento de datos fuera de orden. Flink lee de Kafka, procesa los eventos, y habitualmente escribe los resultados de vuelta a Kafka u otros sistemas de almacenamiento.

**Schema Registry** es un servicio que gestiona los esquemas de los mensajes que fluyen por Kafka. Al registrar el esquema de cada tipo de mensaje (usando Avro, Protobuf o JSON Schema), el Schema Registry garantiza que producers y consumers estén de acuerdo en la estructura de los datos y gestiona la evolución de los esquemas a lo largo del tiempo sin romper la compatibilidad.

**ksqlDB** es un motor de base de datos diseñado específicamente para procesar streams de Kafka usando una sintaxis similar a SQL. Permite crear vistas materializadas sobre streams de eventos, ejecutar queries continuas, y hacer joins y agregaciones sobre datos en tiempo real, todo con una sintaxis familiar para cualquier desarrollador.

---

### Ruta de aprendizaje: por dónde empezar

La curva de aprendizaje de Kafka es más manejable de lo que parece si se sigue un orden progresivo.

**Primeros pasos — Día 1:**

El punto de entrada más rápido es ejecutar Kafka localmente con Docker:

```bash
docker run -p 9092:9092 apache/kafka:4.2.0
```

Con un solo comando hay un broker Kafka funcional disponible en `localhost:9092`. Desde ahí, los siguientes pasos son crear un topic, publicar algunos mensajes con el producer de consola, y leerlos con el consumer de consola. Este ciclo completo se puede hacer en menos de 10 minutos y cubre los conceptos fundamentales de forma práctica.

**Semana 1 — Producer y Consumer con código:**

El paso siguiente es escribir un producer y un consumer en el lenguaje preferido usando el cliente oficial de Kafka. Los clientes oficiales existen para Java, Python, Go, .NET, y otros lenguajes. El objetivo de esta semana es entender el ciclo completo: serialización, configuración de acks, manejo de offsets, y commit manual del offset después de procesar cada mensaje.

**Mes 1 — Consumer Groups y operaciones básicas:**

Con el ciclo básico dominado, el siguiente nivel es entender consumer groups en la práctica: arrancar múltiples instancias del consumer y observar cómo Kafka distribuye las particiones automáticamente. Monitorear el consumer lag. Practicar el reset de offsets para reprocesar eventos históricos. Entender el rebalanceo cuando un consumer entra o sale del grupo.

**Más adelante — Ecosistema:**

Una vez que los conceptos core están sólidos, los siguientes pasos son Kafka Connect para integraciones con sistemas externos, Kafka Streams o Flink para procesamiento de datos, y Schema Registry para gestión de esquemas en equipos y sistemas múltiples.

---

### Resumen: los seis conceptos fundamentales de Kafka

Para consolidar todo lo visto, estos son los seis conceptos que toda persona que trabaje con Kafka debe dominar:

**Topic:** el canal lógico con nombre al que los producers publican eventos y del que los consumers leen. Es la unidad de organización de los datos en Kafka.

**Partición:** la subdivisión física de un topic. Es el mecanismo de escalabilidad horizontal de Kafka. El orden de los mensajes se garantiza dentro de cada partición. La clave del mensaje determina a qué partición va.

**Offset:** el número secuencial que identifica la posición de cada mensaje dentro de una partición. Permite a los consumers saber exactamente dónde están y volver a cualquier punto del historial.

**Producer:** la aplicación que publica mensajes en un topic. Configura la durabilidad mediante la configuración de acks y determina el particionado mediante la clave del mensaje.

**Consumer:** la aplicación que lee mensajes de un topic a su propio ritmo (modelo pull). Hace commit del offset para registrar el progreso y garantizar que no se pierdan mensajes ante fallos.

**Consumer Group:** conjunto de consumers que se dividen las particiones de un topic para procesar en paralelo. Kafka garantiza que cada partición es procesada por exactamente un consumer del grupo. Permite escalar el procesamiento horizontalmente añadiendo más instancias al grupo.

Estos seis conceptos, bien entendidos, son la base para trabajar con Kafka en cualquier proyecto. Todo lo demás — KRaft, Connect, Streams, Schema Registry — son extensiones que se construyen sobre este foundation.

---

*Kafka no es una cola. Es la memoria distribuida de tu sistema.*
