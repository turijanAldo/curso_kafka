# 📖 DECISIONES — Por qué cada archivo, en qué orden y qué hace cada parte

> Este documento acompaña al LAB-GUIDE.md.
> Para cada archivo explica: qué es, por qué existe, por qué en ese orden,
> y qué hace cada parte importante (constantes, campos, métodos).
> Si lees solo el código entiendes el *qué*. Aquí entiendes el *por qué*.

---

## Cómo leer este documento

Cada sección de archivo tiene esta estructura:

```
### nombre-del-archivo.java
  ¿Qué es?
  ¿Por qué existe?
  ¿Por qué antes que otros archivos?
  ¿Qué pasaría si no estuviera?
  → Detalle de cada parte importante (constante / campo / método)
```

---

# PASO 0 — Infraestructura

El PASO 0 crea todo lo que NO es código de negocio: la base de datos, el broker de Kafka
y el esqueleto Maven del proyecto. La regla es simple: **nada de Java hasta que la
infraestructura funcione**. Si empiezas a escribir servicios sin Kafka activo, cada
arranque falla con error de conexión y no puedes verificar nada.

---

## `docker/docker-compose.yml`

**¿Qué es?**
Archivo de configuración de Docker Compose. Describe qué contenedores necesita el proyecto,
cómo se conectan entre sí y cómo verificar que están sanos.

**¿Por qué existe?**
Kafka y MySQL son servicios externos — no los escribimos, los usamos. Sin Docker necesitarías
instalarlos manualmente en tu máquina, configurarlos uno por uno y esperar que funcionen igual
en la máquina de otra persona. Docker Compose resuelve eso: con un solo comando (`docker compose
up -d`) tienes el entorno completo y reproducible.

**¿Por qué se crea antes que cualquier archivo Java?**
Porque Spring Boot intenta conectarse a Kafka y MySQL en el momento de arrancar. Si no existen,
el microservicio lanza una excepción y muere. La infraestructura siempre va antes que el código
que la consume.

**¿Qué pasaría si no estuviera?**
Tendrías que instalar Kafka y MySQL manualmente, configurar puertos, usuarios y contraseñas a mano,
y ese proceso sería diferente en cada máquina del equipo.

### Detalle de cada parte

**`networks: transfers-net`**
Crea una red virtual privada dentro de Docker. Todos los contenedores declarados en este
archivo se ven entre sí por nombre (ej. el contenedor `mysql` puede hablarle al contenedor
`kafka` usando el hostname `kafka`). Sin esta red, los contenedores estarían aislados y no
podrían comunicarse.

**`KAFKA_NODE_ID: 1`**
Identificador único de este nodo Kafka dentro del clúster. En producción un clúster tiene
varios brokers con IDs 1, 2, 3... En este lab hay un solo broker, por eso siempre es 1.
Kafka lo usa internamente para saber quién es el líder de cada partición.

**`KAFKA_PROCESS_ROLES: broker,controller`**
En modo KRaft (sin ZooKeeper), un nodo Kafka puede tener dos roles:
- `broker` → recibe y almacena mensajes de producers/consumers
- `controller` → coordina el estado del clúster (quién lidera cada partición)
En producción se separan en nodos distintos. En este lab un solo nodo hace ambas cosas
porque solo tenemos 1 broker y queremos simplicidad.

**`KAFKA_LISTENERS` y `KAFKA_ADVERTISED_LISTENERS`**
Este es el punto que más confunde. Kafka necesita ser accesible desde dos lugares distintos:

- Desde **dentro de la red Docker** (cuando otro contenedor le habla):
  usa `PLAINTEXT://kafka:29092`. El hostname `kafka` solo existe dentro de la red Docker.
- Desde **tu máquina host** (cuando Spring Boot corre localmente en tu PC):
  usa `EXTERNAL://localhost:9092`. Tu PC no conoce el hostname `kafka`.

`KAFKA_LISTENERS` → dónde Kafka *escucha* (en qué interfaces de red acepta conexiones).
`KAFKA_ADVERTISED_LISTENERS` → qué dirección le *anuncia a los clientes* que deben usar
para conectarse. El cliente recibe esta dirección y la usa para futuras peticiones.

Sin estos dos listeners: o los microservicios locales no se conectan, o los contenedores
Docker no pueden hablarse entre sí.

**`KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093`**
Le dice a Kafka quiénes pueden ser controladores del clúster y dónde están. Formato:
`nodeId@hostname:puerto`. Aquí hay un solo votante: el nodo 1, accesible en `kafka:9093`.
Es el equivalente KRaft de lo que ZooKeeper hacía antes para elegir líderes.

**`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`**
Por defecto Kafka crea un topic automáticamente cuando alguien intenta publicar en uno
que no existe. Esto suena conveniente pero es peligroso: un typo en el nombre del topic
crearía un topic fantasma en lugar de dar un error visible. En este lab creamos los topics
explícitamente para saber exactamente qué existe.

**`KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1`**
Kafka guarda los offsets (posición de lectura de cada consumer) en un topic interno
llamado `__consumer_offsets`. Por defecto ese topic tiene replication-factor 3 (para
tolerancia a fallos). Con 1 solo broker, un RF de 3 es imposible — Kafka lo rechazaría.
Esta variable lo fuerza a 1.

**`healthcheck` en kafka**
```yaml
test: kafka-topics.sh --list --bootstrap-server localhost:9092 || exit 1
interval: 10s
retries: 10
start_period: 30s
```
Docker ejecuta este comando cada 10 segundos. Si tiene éxito, el contenedor pasa a estado
`healthy`. Los `depends_on` de otros servicios pueden esperar a que este healthcheck pase.
`start_period: 30s` le da 30 segundos de gracia al inicio antes de empezar a evaluar
(Kafka tarda en arrancar).

**`mysql volumes: ./init-db.sql:/docker-entrypoint-initdb.d/init-db.sql:ro`**
MySQL tiene una carpeta especial `/docker-entrypoint-initdb.d/`. Todo archivo `.sql` que
encuentre ahí lo ejecuta automáticamente la primera vez que arranca (cuando la DB está vacía).
El `:ro` al final significa *read-only* — el contenedor puede leer el archivo pero no modificarlo.

---

## `docker/init-db.sql`

**¿Qué es?**
Script SQL que MySQL ejecuta automáticamente al crear el contenedor por primera vez.

**¿Por qué existe?**
El lab necesita datos de prueba desde el primer momento: cuentas bancarias con saldo real.
Sin estas cuentas no puedes hacer ninguna transferencia. Este script hace dos cosas:
1. Crea las tablas del sistema
2. Inserta las cuentas iniciales de prueba

**¿Por qué no dejar que solo Flyway cree las tablas?**
Flyway (el gestor de migraciones) crea las tablas cuando Spring Boot arranca. Pero en el
PASO 0 Spring Boot todavía no existe. Este script nos permite verificar que MySQL funciona
y tiene datos antes de escribir una sola línea de Java.

> En producción real los datos iniciales irían en una migración Flyway (`V2__seed_data.sql`),
> no en un script de Docker. Aquí lo hacemos así para simplificar el setup del lab.

**¿Qué pasaría si no estuviera?**
MySQL arrancaría vacío. Tendrías que conectarte manualmente y crear tablas y datos antes de
poder probar cualquier cosa.

### Detalle de cada parte

**Tabla `accounts`**

```sql
id      VARCHAR(36) PRIMARY KEY   -- UUID de la cuenta (ACC-001, ACC-002...)
balance DECIMAL(15,2)             -- DECIMAL y no FLOAT/DOUBLE porque el dinero
                                  -- necesita precisión exacta. FLOAT tiene errores
                                  -- de redondeo (0.1 + 0.2 = 0.30000000004).
                                  -- 15 dígitos totales, 2 decimales.
version BIGINT DEFAULT 0          -- Campo de Optimistic Locking (ver PASO 5).
                                  -- JPA lo usa para detectar modificaciones
                                  -- concurrentes. Cada UPDATE lo incrementa en 1.
```

`CONSTRAINT chk_balance CHECK (balance >= 0)`
Una restricción a nivel de base de datos que impide que el saldo sea negativo.
Es la última línea de defensa — incluso si la lógica Java tiene un bug y no valida
el saldo, la DB rechazará el UPDATE con un error. Siempre es mejor tener esta
protección en la capa más baja.

**Tabla `transactions`**

```sql
status VARCHAR(30) DEFAULT 'PROCESSING'
-- Guarda el estado actual del Saga para esta transferencia.
-- Valores posibles: PROCESSING, VALIDATED, DEBITED, CREDITED,
--                   COMPLETED, FAILED, ROLLED_BACK

CONSTRAINT chk_status CHECK (status IN (...))
-- Igual que con el balance: la DB valida que el status sea uno de los
-- valores permitidos. Si un bug en Java intenta guardar "DONE" o "OK",
-- la DB lo rechaza.

INDEX idx_tx_status (status)
-- Índice en la columna status porque status-service va a hacer
-- SELECT * FROM transactions WHERE status = 'PROCESSING' frecuentemente.
-- Sin índice, esa query haría un full table scan (leer toda la tabla).
-- Con índice, MySQL va directo a las filas relevantes.
```

**Tabla `processed_events`**

Esta tabla es el corazón de la **idempotencia**. Su propósito es recordar qué eventos
ya fueron procesados para no procesarlos dos veces.

```sql
event_key    VARCHAR(200)   -- Identificador único del evento procesado.
                            -- Formato: transactionId + ":" + tipoEvento
                            -- Ejemplo: "f47ac10b-...:TRANSFER_DEBITED"
service_name VARCHAR(50)    -- Qué microservicio lo procesó.
                            -- Porque el mismo event_key puede existir en
                            -- validation-service Y en account-service por separado.

CONSTRAINT uq_event_key UNIQUE (event_key, service_name)
-- LA RESTRICCIÓN MÁS IMPORTANTE DEL SISTEMA.
-- Si dos threads del mismo servicio reciben el mismo evento al mismo tiempo,
-- ambos pasarán el chequeo de "¿ya existe?" porque la DB aún no tiene el registro.
-- Pero solo uno podrá hacer el INSERT exitosamente.
-- El segundo recibirá un error de clave duplicada (DuplicateKeyException).
-- Esto es la Capa 3 de idempotencia — la DB como árbitro final.
```

**Datos semilla**

```sql
INSERT INTO accounts (id, owner_name, balance, version) VALUES
    ('ACC-001', 'Ana García',   1000.00, 0),
    ('ACC-002', 'Bob Martínez',  500.00, 0),
    ('ACC-003', 'Carlos López', 2500.00, 0);
```

Tres cuentas con saldos distintos para poder probar escenarios variados:
- `ACC-001 → ACC-002`: transferencia normal (Ana tiene suficiente)
- `ACC-002 → ACC-001`: transferencia de 600 (Bob no tiene suficiente → FAILED)
- `ACC-001 → ACC-003`: transferencia grande para probar rollback simulado

---

## `docker/wait-for-kafka.ps1`

**¿Qué es?**
Script PowerShell que hace polling al puerto TCP de Kafka hasta que responde o agota el tiempo.

**¿Por qué existe?**
Cuando haces `docker compose up -d`, los contenedores arrancan pero no están listos de inmediato.
Kafka tarda 15-30 segundos en inicializar KRaft. Si en ese tiempo intentas arrancar Spring Boot,
el servicio intenta conectarse, falla y puede no recuperarse automáticamente.

**¿Por qué TCP y no simplemente esperar a que `docker compose ps` diga `healthy`?**
El healthcheck de Docker verifica que el proceso está vivo, pero no que acepta conexiones
de clientes externos. La única forma segura de saberlo es intentar conectarse al puerto.

**¿Qué pasaría si no estuviera?**
Tendrías que esperar manualmente y adivinar cuándo Kafka está listo, o agregar un
`Start-Sleep -Seconds 30` fijo que a veces es demasiado poco y a veces demasiado.

### Detalle de cada parte

**Parámetros del script**
```powershell
param(
    [string]$KafkaHost = "localhost",  -- Host donde buscar Kafka.
                                       -- Default: localhost (desarrollo local).
    [int]$Port         = 9092,         -- Puerto estándar de Kafka para clientes.
    [int]$TimeoutSec   = 120,          -- Máximo 2 minutos de espera.
                                       -- Si Kafka no arranca en 2 min, algo falló.
    [int]$IntervalSec  = 2             -- Revisar cada 2 segundos.
                                       -- Menos de 2s satura el log innecesariamente.
)
```

**El intento de conexión TCP**
```powershell
$tcpClient = New-Object System.Net.Sockets.TcpClient
$connect   = $tcpClient.BeginConnect($KafkaHost, $Port, $null, $null)
$waited    = $connect.AsyncWaitHandle.WaitOne(1000, $false)
```
- `TcpClient` es la clase de .NET que abre conexiones TCP crudas (sin HTTP, sin Kafka).
- `BeginConnect` inicia la conexión de forma asíncrona.
- `WaitOne(1000, false)` espera máximo 1 segundo la respuesta.
- Si `$waited` es `$true` Y `$tcpClient.Connected` es `$true`, Kafka está escuchando.

**¿Por qué no usar `Test-NetConnection` de PowerShell directamente?**
`Test-NetConnection` existe pero imprime texto en la consola que no podemos suprimir
fácilmente en versiones antiguas de PowerShell. El enfoque con `TcpClient` es silencioso
y funciona en todas las versiones de PowerShell 5+.

**Códigos de salida `exit 0` y `exit 1`**
Por convención en sistemas Unix/Windows, un proceso que termina con código 0 tuvo éxito,
cualquier otro código indica error. Esto permite encadenar el script en un pipeline:
```powershell
.\wait-for-kafka.ps1 && mvn spring-boot:run
# Si wait-for-kafka termina con 0 (éxito), arranca el microservicio
# Si termina con 1 (timeout), el && no ejecuta el siguiente comando
```

---

# PASO 0.B — Maven Multi-Module

## `kafka-transfers/pom.xml` (POM padre)

**¿Qué es?**
El archivo de configuración raíz del proyecto Maven. No produce código — solo coordina.

**¿Por qué existe un POM padre y no un POM por microservicio de forma independiente?**
Sin POM padre tendrías 5 proyectos Maven completamente separados. Para compilar el sistema
completo necesitarías entrar a 5 carpetas distintas y ejecutar `mvn compile` en cada una, en
el orden correcto (common primero, luego los servicios). El POM padre automatiza eso: desde
la raíz un solo `mvn compile` compila todo en el orden correcto.

**¿Qué pasaría si no estuviera?**
Cada microservicio declararía su propia versión de Spring Boot. En un proyecto con 5 personas,
en 3 meses tendrías 5 versiones distintas de Spring Boot, 3 versiones de Kafka client y 2
versiones de MySQL connector. Los conflictos de dependencias son difíciles de diagnosticar.

### Detalle de cada parte

**`<packaging>pom</packaging>`**
Le dice a Maven que este proyecto NO produce un JAR ni un WAR. Es solo un coordinador.
Si pusiera `<packaging>jar</packaging>`, Maven buscaría código Java en la raíz y fallaría
porque no hay ninguno.

**`<modules>` — el orden importa**
```xml
<modules>
    <module>common</module>           <!-- PRIMERO: los otros 4 dependen de él -->
    <module>transfer-api</module>
    <module>validation-service</module>
    <module>account-service</module>
    <module>status-service</module>
</modules>
```
Maven compila en este orden. Si `transfer-api` fuera primero, Maven intentaría compilarlo,
encontraría que depende de `com.lab:common` y buscaría ese JAR en `~/.m2/repository`.
No lo encontraría porque aún no se compiló, y fallaría. `common` debe ir primero siempre.

**`<dependencyManagement>` vs `<dependencies>`**
Esta distinción es fundamental en Maven:

- `<dependencyManagement>` → **declara** versiones disponibles, pero NO las importa.
  Los módulos hijos todavía tienen que pedir la dependencia, pero no necesitan la versión
  porque la heredan del padre.

- `<dependencies>` en el padre → **importa** la dependencia en TODOS los módulos hijos
  automáticamente, sin que ellos la pidan.

En este POM:
```xml
<!-- En <dependencyManagement>: versiones declaradas, no importadas automáticamente -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-dependencies</artifactId>  <!-- El BOM de Spring Boot -->
    <version>${spring-boot.version}</version>
    <type>pom</type>
    <scope>import</scope>  <!-- "importar" el BOM: carga todas las versiones que
                               Spring Boot recomienda para sus dependencias internas -->
</dependency>

<!-- En <dependencies>: esto SÍ se hereda por todos los hijos sin pedirlo -->
<dependency>
    <groupId>org.projectlombok</groupId>
    <artifactId>lombok</artifactId>
    <scope>provided</scope>  <!-- Solo en compilación, no va al JAR final -->
</dependency>
```

**¿Qué es el Spring Boot BOM (`spring-boot-dependencies`)?**
BOM = Bill Of Materials. Es un POM especial que Spring Boot publica con una lista de
cientos de dependencias y sus versiones exactas que son compatibles entre sí. Al importarlo,
no necesitas especificar la versión de Jackson, Hibernate, Kafka Client, Tomcat, etc.
Spring Boot garantiza que las versiones que elige no tienen conflictos entre ellas.

**`<scope>provided</scope>` en Lombok**
Significa que Lombok está disponible durante la compilación pero NO se incluye en el JAR
final. Lombok es un procesador de anotaciones que genera código en tiempo de compilación.
En el JAR resultante ya están los getters/setters/builders generados — la librería Lombok
en sí ya no es necesaria en tiempo de ejecución.

---

# PASO 1 — Módulo Common

El módulo `common` es el **contrato compartido** del sistema. Define el lenguaje que todos
los microservicios usan para comunicarse. Si cambias cualquier cosa aquí, todos los
servicios que lo usan lo saben en tiempo de compilación.

---

## `common/pom.xml`

**¿Qué es?**
POM del módulo common. Define cómo se empaqueta y qué necesita para compilar.

**¿Por qué existe un módulo common separado y no copiamos las clases en cada servicio?**
Si copiaras `TransferRequestedEvent.java` en cada microservicio, en algún momento alguien
cambiaría el nombre de un campo en un servicio pero no en otro. `transfer-api` publica
`{"transactionId": "..."}` pero `validation-service` espera `{"txId": "..."}`. El JSON
no matchea, el campo llega como `null` y nadie entiende por qué. El módulo `common`
garantiza que todos usan exactamente la misma definición. Un cambio ahí lo ven todos.

**¿Por qué `<packaging>jar</packaging>` y no `pom`?**
Al contrario del padre, `common` sí produce un JAR (`common-1.0.0.jar`). Este JAR es lo
que los otros módulos importan. Si fuera `pom`, no habría JAR y los otros módulos no
podrían usar las clases de `common`.

**¿Por qué NO tiene `spring-boot-maven-plugin`?**
El `spring-boot-maven-plugin` crea fat JARs ejecutables — empaqueta la JVM completa más
todas las dependencias en un solo archivo. `common` es una librería, no una aplicación.
Los otros módulos necesitan importar `common` como librería estándar. Un fat JAR de Spring
Boot tiene una estructura interna especial que Maven no puede usar como dependencia normal.

**¿Por qué tiene `jakarta.validation-api` como dependencia?**
Los eventos usan anotaciones de validación como `@NotNull` y `@NotBlank` en sus campos.
Estas anotaciones viven en `jakarta.validation-api`. Como los eventos están en `common`,
`common` necesita esta dependencia para compilar, aunque la validación real se ejecute
en los microservicios que usan los eventos.

---

## `enums/TransferStatus.java`

**¿Qué es?**
Una enumeración Java con los 7 estados posibles de una transferencia a través del Saga.

**¿Por qué un enum y no Strings (`"PROCESSING"`, `"COMPLETED"`)?**
Con Strings nada impide escribir `"Completed"` en un servicio y `"COMPLETED"` en otro.
La base de datos los trata como valores distintos. El compilador no detecta el error.
Con un enum, si escribes `TransferStatus.Completd`, el compilador falla inmediatamente
con "cannot find symbol". Los errores en tiempo de compilación son infinitamente mejores
que los errores en producción.

**¿Por qué 7 estados y no solo PENDING / DONE / FAILED?**
Porque esta es una saga distribuida en 4 servicios. Necesitas saber exactamente en qué
punto del proceso está una transferencia para decidir qué hacer si algo falla.

Si alguien pregunta "¿esta transferencia que está en FAILED, ya se le había debitado
el dinero a Ana?" y solo tienes FAILED como respuesta, no puedes saberlo. Con los 7
estados sí puedes: si está en FAILED y el estado anterior fue DEBITED, entonces sí —
hay que revertir el débito. Si está en FAILED y el estado anterior fue VALIDATED,
no hay nada que revertir.

**Detalle de cada valor del enum**

```java
PROCESSING   // La transferencia fue recibida por transfer-api y publicada en Kafka.
             // Ningún servicio la ha procesado aún. Es el estado inicial de toda
             // transferencia que existe en la tabla transactions.

VALIDATED    // validation-service revisó que: la cuenta origen existe, la cuenta destino
             // existe, el monto es positivo, y las cuentas son distintas.
             // account-service puede debitar con confianza.

DEBITED      // account-service restó el monto del saldo de la cuenta origen.
             // PUNTO CRÍTICO: el dinero ya salió. Si algo falla de aquí en adelante,
             // DEBE ejecutarse una transacción de compensación para devolver el dinero.

CREDITED     // account-service sumó el monto al saldo de la cuenta destino.
             // El dinero llegó a destino. Solo queda que status-service confirme.

COMPLETED    // status-service recibió el evento TRANSFER_CREDITED y cerró el Saga
             // exitosamente. Estado terminal — no hay transición posible desde aquí.

FAILED       // Algo salió mal en algún paso. Este estado puede aparecer desde
             // PROCESSING (validación rechazada) hasta DEBITED (crédito falló).
             // Si venía de DEBITED, el siguiente estado será ROLLED_BACK.

ROLLED_BACK  // account-service ejecutó la compensación: devolvió el dinero a la cuenta
             // origen. Estado terminal. La transferencia fracasó pero el sistema quedó
             // consistente (nadie perdió dinero).
```

**Máquina de estados — flujo completo**
```
[entrada]
    ↓
PROCESSING ──► VALIDATED ──► DEBITED ──► CREDITED ──► COMPLETED  [fin feliz]
    │               │             │
    ↓               ↓             ↓
  FAILED          FAILED        FAILED ──► ROLLED_BACK             [fin con error]
(sin débito)  (sin débito)   (con débito,
                               necesita
                               compensación)
```

---

## `constants/KafkaTopics.java`

**¿Qué es?**
Una clase Java con 7 constantes String — los nombres de todos los topics Kafka del sistema.

**¿Por qué existe esta clase y no escribimos el nombre del topic directamente en el código?**
El problema real: si en `transfer-api` escribes `"transfer.requested"` y en
`validation-service` escribes `"transfer.requsted"` (typo), el sistema compila y arranca
sin ningún error. Kafka simplemente no entrega ningún mensaje a `validation-service` porque
nadie está escuchando en ese topic mal escrito. Este tipo de bug puede tardar horas en
diagnosticarse.

Con la clase de constantes:
```java
// En transfer-api (producer):
kafkaTemplate.send(KafkaTopics.TRANSFER_REQUESTED, event);

// En validation-service (consumer):
@KafkaListener(topics = KafkaTopics.TRANSFER_REQUESTED)
```
Ambos referencian la misma constante. Si el topic cambia de nombre, lo cambias en un solo
lugar y el compilador te dice exactamente dónde se usa.

**¿Por qué `final class` con `private constructor()`?**
```java
public final class KafkaTopics {
    private KafkaTopics() { }  // Nadie puede hacer: new KafkaTopics()
    ...
}
```
Esta clase no tiene estado — solo agrupa constantes. El patrón `final class` + constructor
privado impide que alguien la instancie o la extienda accidentalmente. `final` además
prohíbe herencia. Es el patrón estándar en Java para "clases de utilidades".

**Detalle de cada constante**

```java
TRANSFER_REQUESTED = "transfer.requested"
// Publicado por: transfer-api cuando recibe un POST /transfers
// Consumido por: validation-service
// Propósito: arrancar el Saga. Es el primer evento de toda transferencia.
// Partición por: fromAccount (todas las transferencias de la misma cuenta
//                origen van a la misma partición → orden garantizado)

TRANSFER_VALIDATED = "transfer.validated"
// Publicado por: validation-service cuando la validación es exitosa
// Consumido por: account-service
// Propósito: señal de "luz verde" para proceder con el débito
// Si la validación falla, se publica TRANSFER_FAILED en su lugar

TRANSFER_FAILED = "transfer.failed"
// Publicado por: validation-service (rechazo) o account-service (error)
// Consumido por: status-service
// Propósito: señalizar el fin del Saga por la ruta de error
// status-service actualiza el estado a FAILED en la tabla transactions

TRANSFER_DEBITED = "transfer.debited"
// Publicado por: account-service después de debitar la cuenta origen
// Consumido por: account-service mismo (para proceder al crédito)
//               status-service (para actualizar estado a DEBITED)
// Propósito: confirmar que el paso más crítico del Saga ocurrió
// IMPORTANTE: un consumer group distinto en account-service para crédito
//             y otro en status-service aseguran que AMBOS lo procesan

TRANSFER_CREDITED = "transfer.credited"
// Publicado por: account-service después de acreditar la cuenta destino
// Consumido por: status-service
// Propósito: señalizar el fin exitoso del Saga
// status-service actualiza el estado a COMPLETED

TRANSFER_COMPENSATED = "transfer.compensated"
// Publicado por: account-service después de REVERTIR el débito
// Consumido por: status-service
// Propósito: señalizar que la compensación fue exitosa
// status-service actualiza el estado a ROLLED_BACK
// Solo existe si el crédito falló después de que el débito ya fue aplicado

TRANSFER_DLQ = "transfer.dlq"
// DLQ = Dead Letter Queue
// Publicado por: cualquier servicio que agota sus reintentos
// Propósito: "estacionamiento" de mensajes que no pudieron procesarse
// En producción: un sistema de alertas los monitorea y notifica al equipo
// En este lab: se crea el topic pero no se implementa el consumer
// Tiene solo 1 partición porque el orden no importa aquí
```

---

## Los 6 eventos del Saga

### ¿Por qué 6 clases distintas y no una clase genérica `KafkaEvent`?

Esta es la pregunta de diseño más importante del módulo `common`. Podrías hacer:

```java
// OPCIÓN MALA — un evento genérico con todos los campos posibles
class KafkaEvent {
    String  type;                 // "TRANSFER_REQUESTED", "TRANSFER_DEBITED", etc.
    String  transactionId;
    String  fromAccount;
    String  toAccount;
    BigDecimal amount;
    String  reason;               // solo aplica para FAILED
    BigDecimal remainingBalance;  // solo aplica para DEBITED
    String  compensationReason;   // solo aplica para COMPENSATED
    BigDecimal restoredBalance;   // solo aplica para COMPENSATED
    String  failedBy;             // solo aplica para FAILED
}
```

Problemas de este enfoque:
1. **Campos siempre vacíos**: si `type = "TRANSFER_REQUESTED"`, los campos `reason`,
   `remainingBalance` y `compensationReason` están vacíos sin ningún significado.
2. **Sin contrato claro**: el developer que implementa `validation-service` no sabe
   qué campos esperar. Tiene que leer el código del producer para entenderlo.
3. **Imposible evolucionar sin romper**: si agregas un campo nuevo para `TRANSFER_DEBITED`,
   lo agregas a todos los tipos aunque no aplique. Todos los consumers necesitan actualizarse.
4. **Sin validación posible**: no puedes poner `@NotNull` en `reason` porque solo es
   obligatorio en eventos de tipo FAILED, no en los demás.

Con 6 clases distintas:
- Cada evento tiene exactamente los campos que necesita
- El Javadoc de cada clase documenta el flujo completo
- Puedes agregar campos a `TransferDebitedEvent` sin tocar `TransferRequestedEvent`
- El compilador valida que el consumer recibe el tipo correcto

---

## `event/TransferRequestedEvent.java`

**¿Qué es?**
El primer evento del Saga. Representa una solicitud de transferencia recién recibida.

**¿Por qué se crea primero entre los eventos?**
Porque es el punto de entrada del Saga. Entender sus campos explica el flujo completo.
Los otros eventos son variaciones o extensiones de este contrato inicial.

**¿Qué pasaría si no estuviera?**
`transfer-api` no podría publicar nada a Kafka. El Saga no arrancaría.

### Detalle de cada campo

```java
String transactionId
// UUID generado por transfer-api al recibir el POST /transfers.
// Es el IDENTIFICADOR DEL SAGA COMPLETO. Cada evento que se publique
// a lo largo del flujo lleva este mismo transactionId. Permite rastrear
// una transferencia de extremo a extremo en los logs de 4 servicios distintos.
// También es la base del event_key de idempotencia:
//   event_key = transactionId + ":" + tipo_evento

String fromAccount
// ID de la cuenta que envía. Ejemplo: "ACC-001"
// DOBLE PROPÓSITO:
// 1. Dato de negocio para saber a quién debitar
// 2. CLAVE DE PARTICIÓN KAFKA: al publicar el evento, transfer-api
//    usa fromAccount como clave. Kafka garantiza que todos los mensajes
//    con la misma clave van a la misma partición. Resultado: todas las
//    transferencias de la misma cuenta origen se procesan EN ORDEN.
//    Sin esto, dos transferencias de ACC-001 podrían procesarse en paralelo
//    y causar condiciones de carrera en el saldo.

String toAccount
// ID de la cuenta que recibe. Ejemplo: "ACC-002"

BigDecimal amount
// Monto a transferir. BigDecimal y no double ni float.
// ¿Por qué? Porque double tiene errores de punto flotante:
//   0.1 + 0.2 = 0.30000000000000004 en double
//   0.1 + 0.2 = 0.3 en BigDecimal
// El dinero necesita precisión exacta. Un error de 0.000001 en miles
// de transacciones se convierte en un problema contable real.

Instant timestamp
// Momento en UTC cuando se creó el evento.
// ¿Por qué Instant y no LocalDateTime?
// LocalDateTime no tiene zona horaria. Si transfer-api corre en México
// (UTC-6) y status-service corre en un servidor en Europa (UTC+1), sus
// LocalDateTime serían incomparables. Instant siempre es UTC.
// ¿Por qué @JsonFormat(shape = STRING)?
// Le dice a Jackson que serialice el Instant como "2024-01-15T10:30:00Z"
// (texto legible) en lugar de 1705316600 (timestamp numérico).
// El texto es legible en los logs de Kafka y parseable por cualquier lenguaje.
```

**Anotaciones de Lombok y por qué van juntas**

```java
@Data               // Genera: getters, setters, equals(), hashCode(), toString()
@Builder            // Genera: TransferRequestedEvent.builder()...build()
                    // Permite construir el objeto campo por campo sin constructores largos
@NoArgsConstructor  // Genera: constructor vacío TransferRequestedEvent()
                    // Jackson NECESITA esto para deserializar JSON → objeto Java.
                    // Sin él, Jackson lanza: "No suitable constructor found"
@AllArgsConstructor // Genera: constructor con todos los campos
                    // @Builder lo necesita internamente para funcionar.
                    // Si tienes @Builder sin @AllArgsConstructor, el builder
                    // genera un error de compilación extraño.
```

> **Regla**: si usas `@Builder`, siempre acompáñalo con `@NoArgsConstructor` y
> `@AllArgsConstructor`. Son los tres juntos o no funciona correctamente con Jackson.

---

## `event/TransferValidatedEvent.java`

**¿Qué es?**
Confirmación de que la transferencia pasó todas las validaciones.

**¿Por qué repite `fromAccount`, `toAccount` y `amount` si validation-service ya los
recibió en `TransferRequestedEvent`?**
Esto se llama **Event-Carried State Transfer**. En lugar de que `account-service` haga
una consulta HTTP o a la base de datos para obtener los datos de la transferencia cuando
recibe este evento, los datos viajan dentro del evento mismo. Ventajas:
- Sin acoplamiento HTTP entre servicios (coreografía pura, nadie llama a nadie)
- Sin latencia de consultas adicionales
- Si la DB está temporalmente caída, el mensaje ya tiene todo lo que necesita
- Los servicios son más independientes y fáciles de escalar

---

## `event/TransferFailedEvent.java`

**¿Qué es?**
Señal de que el Saga terminó por la ruta de error.

**Campos exclusivos de este evento**

```java
String reason
// Mensaje legible del motivo del fallo. Ejemplos reales:
//   "Cuenta origen no encontrada: ACC-999"
//   "Saldo insuficiente. Disponible: 50.00, Requerido: 200.00"
//   "La cuenta origen y destino son la misma"
// Este mensaje puede mostrarse directamente al usuario en la UI.

String failedBy
// Qué microservicio detectó el error. Valores posibles:
//   "validation-service" → falló antes del débito (no hay nada que revertir)
//   "account-service"    → falló durante el crédito (puede haber que revertir)
// status-service usa este campo para decidir si esperar una compensación o no.
```

---

## `event/TransferDebitedEvent.java`

**¿Qué es?**
Confirmación de que el dinero salió de la cuenta origen.

**¿Por qué este evento es el más crítico del Saga?**
Porque después de publicarlo, el dinero ya no está en la cuenta de Ana. Si cualquier paso
posterior falla, el sistema DEBE revertir este débito. Sin este evento claramente definido,
no hay forma de saber si la compensación es necesaria o no.

**Campo exclusivo**

```java
BigDecimal remainingBalance
// Saldo restante en la cuenta origen después del débito.
// Ejemplo: Ana tenía 1000.00, transfiere 200.00 → remainingBalance = 800.00
// Es informativo: útil para logs, auditoría y monitoreo.
// No es necesario para el flujo del Saga, pero ayuda a diagnosticar
// si algo sale mal ("¿cuánto tenía exactamente cuando se debitó?").
```

---

## `event/TransferCreditedEvent.java`

**¿Qué es?**
Confirmación de que el dinero llegó a la cuenta destino. Es el último evento de la ruta feliz.

**Campo exclusivo**

```java
BigDecimal newBalance
// Nuevo saldo de la cuenta destino después del crédito.
// Ejemplo: Bob tenía 500.00, recibe 200.00 → newBalance = 700.00
// Mismo propósito informativo que remainingBalance en TransferDebitedEvent.
```

---

## `event/TransferCompensatedEvent.java`

**¿Qué es?**
Confirmación de que el débito previo fue revertido exitosamente (transacción de compensación).

**¿Por qué existe este evento y no se reutiliza `TransferFailedEvent`?**
Porque representan momentos distintos del Saga:
- `TransferFailedEvent` → "algo salió mal, el Saga entra en error"
- `TransferCompensatedEvent` → "el error ya fue corregido, el saldo fue restaurado"

`status-service` necesita distinguirlos para saber si el estado final es `FAILED`
(el error ocurrió pero el débito aún no fue revertido) o `ROLLED_BACK` (el débito
fue revertido exitosamente y el sistema quedó consistente).

**Campos exclusivos**

```java
String compensationReason
// Qué salió mal antes de que se iniciara la compensación.
// Ejemplo: "Error al acreditar en cuenta destino ACC-002: cuenta bloqueada"
// Permite rastrear la causa raíz del rollback en los logs.

BigDecimal restoredBalance
// Saldo de la cuenta origen DESPUÉS de revertir el débito.
// Debe ser igual al saldo antes de TransferDebitedEvent.
// Si no es igual, hay un bug serio en la lógica de compensación.
// Útil para verificación en auditorías.
```

---

## Orden de creación del PASO 1 — Por qué este orden

```
common/pom.xml              ← primero: define cómo se empaqueta el módulo
TransferStatus.java         ← segundo: los eventos hacen referencia al enum
KafkaTopics.java            ← tercero: los topics los referencian los eventos (Javadoc)
TransferRequestedEvent.java ← cuarto: es el primero del flujo del Saga
TransferValidatedEvent.java ← quinto: depende conceptualmente del anterior
TransferFailedEvent.java    ← sexto: puede salir de cualquier punto del flujo
TransferDebitedEvent.java   ← séptimo: sigue el orden del Saga
TransferCreditedEvent.java  ← octavo: penúltimo paso del flujo exitoso
TransferCompensatedEvent.java ← último: concepto más complejo (compensación)
```

**Principio guía de todo el proyecto:** cada archivo depende solo de lo que ya existe.
Nunca se crea algo que hace referencia a algo que aún no fue creado.

---

---

# PASO 2 — POMs, application.yml y migraciones Flyway

El PASO 2 establece la capa de datos y configuración de cada microservicio antes
de escribir una sola línea de lógica de negocio. Tres tipos de archivos por servicio:
`pom.xml` (qué necesita), `application.yml` (cómo se conecta), `V1__.sql` (qué tablas crea).

---

## `{servicio}/pom.xml` — los 4 POMs de microservicio

**¿Por qué cada microservicio tiene su propio pom.xml?**
Cada servicio es un JAR independiente que puede arrancar, escalar y desplegarse por
separado. Su `pom.xml` declara exactamente lo que ese servicio necesita y nada más.
Esto mantiene los JARs pequeños y las dependencias explícitas.

**¿Por qué `spring-boot-maven-plugin` con `<goal>repackage</goal>`?**
```xml
<plugin>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-maven-plugin</artifactId>
    <executions>
        <execution>
            <goals><goal>repackage</goal></goals>
        </execution>
    </executions>
</plugin>
```
`repackage` convierte el JAR estándar de Maven en un **fat JAR ejecutable** —
empaqueta el código del servicio junto con todas sus dependencias en un solo archivo.
Resultado: puedes correr el servicio con `java -jar transfer-api-1.0.0.jar` sin
instalar nada más en el servidor. El módulo `common` NO tiene este plugin porque
es una librería, no una aplicación.

**¿Por qué `validation-service` y `account-service` NO tienen `spring-boot-starter-web`?**
`spring-boot-starter-web` incluye Tomcat embebido y todo el stack MVC. Si un servicio
no expone endpoints HTTP, incluirlo agrega ~5MB al JAR, tiempo de arranque innecesario
y un puerto HTTP expuesto sin utilidad. Estos dos servicios solo escuchan topics Kafka
y escriben en DB — no necesitan HTTP. Spring Boot arranca igualmente gracias a
`spring-boot-starter-data-jpa` y `spring-kafka`.

**¿Por qué `transfer-api` tiene `spring-boot-starter-validation`?**
Esta dependencia activa el motor de Bean Validation (Hibernate Validator) que
procesa anotaciones como `@NotNull`, `@Positive`, `@Size` en los DTOs de request.
Sin ella, las anotaciones de validación están en el código pero nunca se ejecutan —
cualquier valor, incluso `null` o negativos, pasaría al servicio sin ser rechazado.

---

## `application.yml` — los 4 archivos de configuración

**¿Por qué YAML y no `.properties`?**
Ambos formatos funcionan igual con Spring Boot. YAML permite jerarquías anidadas y
es más legible para configuraciones con sub-secciones como Kafka (consumer, producer,
listener). En proyectos reales YAML es el estándar actual.

**`spring.jpa.hibernate.ddl-auto: validate`**
Controla qué hace Hibernate con el schema de la DB al arrancar:

| Valor | Qué hace | Cuándo usar |
|-------|----------|-------------|
| `create` | Borra todo y crea de nuevo | Solo en tests unitarios |
| `update` | Intenta alterar tablas existentes | Nunca en producción |
| `validate` | Solo verifica que entidades coincidan con el schema | **Este lab y producción** |
| `none` | No hace nada | Cuando Flyway maneja todo |

Usamos `validate` porque Flyway ya creó las tablas. Si hubiera un mismatch entre
la entidad Java y la tabla real (ej: olvidaste agregar una columna en el SQL),
Hibernate lo detecta al arrancar y falla con un mensaje claro antes de procesar
cualquier petición.

**`spring.flyway.table: flyway_history_{servicio}`**
Flyway guarda el historial de qué migraciones ya ejecutó en una tabla en la DB.
Por defecto esa tabla se llama `flyway_schema_history`. El problema: todos los
servicios comparten `transfers_db`.

Sin este parámetro:
1. `transfer-api` arranca → registra su `V1` en `flyway_schema_history`
2. `account-service` arranca → ve `V1` ya registrado → NO ejecuta su propio `V1`
   → `accounts` y `processed_events` nunca se crean → el servicio falla

Con nombre único por servicio, cada uno lleva su historial aislado y ejecuta
sus propias migraciones sin importar el orden de arranque.

**`enable.auto.commit: false` + `ack-mode: MANUAL_IMMEDIATE`**
Cuando Kafka entrega un mensaje, el consumer debe confirmar (commit) el offset
para decirle a Kafka "ya lo procesé, no me lo mandes de nuevo". Hay dos modos:

Con `auto.commit: true` (peligroso):
```
Consumer recibe mensaje → aplica débito en DB ✅ → intenta publicar evento ❌
Kafka hace auto-commit del offset automáticamente a los 5 segundos
Consumer reinicia → Kafka NO reenvía el mensaje → el débito ocurrió pero el
evento Kafka nunca se publicó → sistema inconsistente, sin forma de recuperarse
```

Con `auto.commit: false` + confirmación manual (seguro):
```
Consumer recibe mensaje → aplica débito ✅ → publica evento ✅
→ llama acknowledgment.acknowledge() → Kafka confirma offset
Si falla en cualquier paso antes del acknowledge → offset no se confirma
→ Kafka reenvía el mensaje al reiniciar → idempotencia lo maneja
```

**`auto.offset.reset: earliest`**
Cuando un consumer group arranca por primera vez, ¿desde dónde lee?
- `earliest` → desde el primer mensaje disponible en el topic (no pierde nada)
- `latest` → solo mensajes nuevos a partir del arranque

Usamos `earliest` en el lab para garantizar que no se pierden mensajes entre
reinicios durante el desarrollo.

**`spring.json.trusted.packages: com.lab.*`**
El deserializador JSON de Spring Kafka rechaza por defecto clases de paquetes
desconocidos (protección contra ataques de deserialización maliciosa). Al agregar
`com.lab.*`, permite deserializar todos los eventos de nuestro módulo `common`.

**`app.simulate-credit-failure: false`** (solo account-service)
Flag custom del lab. `CreditService.java` tiene:
```java
@Value("${app.simulate-credit-failure:false}")
private boolean simulateCreditFailure;
// Si es true, lanza RuntimeException después del débito
// para demostrar la compensación del Saga en el PASO 8
```
La razón de hacerlo por config y no por código hardcodeado: en producción real
los feature flags se controlan desde el exterior del servicio (variables de entorno,
feature flag service). Al usar `@Value`, puedes activarlo con:
`java -jar account-service.jar --app.simulate-credit-failure=true`
sin recompilar nada.

---

## `V1__{nombre}.sql` — los 4 scripts de migración

**¿Por qué Flyway y no `ddl-auto: create`?**
`ddl-auto: create` destruye y recrea toda la DB cada vez que el servicio arranca.
Parece cómodo en desarrollo pero tiene problemas graves:

1. **Borra datos en cada reinicio** → pierdes los datos de prueba cada vez que
   reinicias para ver un log
2. **Sin historial** → no puedes saber qué cambió en el schema entre versiones
3. **Imposible en producción** → no puedes destruir la DB de producción al desplegar

Flyway aplica migraciones **incrementales y versionadas**. Si el schema está en
versión 3, solo aplica V4 en adelante. Los datos existentes se preservan.
El historial queda registrado con fecha, checksum y estado de cada migración.

**¿Por qué el nombre tiene dos guiones bajos `V1__nombre.sql`?**
Convención de Flyway para separar las partes del nombre:
- `V` → versión (también existe `U` para undo y `R` para repetible)
- `1` → número que determina el orden de ejecución
- `__` → separador obligatorio (DOS guiones bajos, no uno)
- `nombre` → descripción libre, solo para legibilidad humana

**¿Por qué `CREATE TABLE IF NOT EXISTS` en todos?**
Porque los servicios comparten la misma DB y pueden arrancar en cualquier orden.
`IF NOT EXISTS` hace cada migración idempotente: si la tabla ya existe (creada por
`init-db.sql` o por otro servicio), MySQL la ignora silenciosamente.
Sin `IF NOT EXISTS`, el segundo servicio en arrancar fallaría con "Table already exists".

**`V1__create_account_schema.sql` — por qué el campo `version` en `accounts`**
```sql
version BIGINT NOT NULL DEFAULT 0
```
Este campo es el soporte de **Optimistic Locking**. JPA (`@Version` en la entidad)
lo incrementa automáticamente en cada UPDATE. El UPDATE generado por Hibernate:
```sql
UPDATE accounts SET balance = 800, version = 6
WHERE id = 'ACC-001' AND version = 5
```
Si entre la lectura y el UPDATE otro thread ya modificó la fila (version ya es 6),
el `WHERE id='ACC-001' AND version=5` no encuentra la fila → 0 filas actualizadas
→ JPA lanza `OptimisticLockException` → Spring Kafka reintenta el mensaje.
Esto previene que dos débitos simultáneos corrompan el saldo sin usar `SELECT FOR UPDATE`
(que bloquearía la fila y reduciría el throughput).

**`V1__create_account_schema.sql` — por qué `UNIQUE (event_key, service_name)` en `processed_events`**
Esta restricción es la **Capa 3 de idempotencia** — la más importante del sistema.
La restricción `UNIQUE` es atómica a nivel de motor de base de datos: incluso con
100 threads intentando insertar el mismo `(event_key, service_name)` simultáneamente,
MySQL garantiza que solo 1 lo logra. Los demás reciben `DuplicateKeyException`,
que el código captura e ignora. Sin esta restricción, la Capa 2 (el `SELECT` previo
que verifica si ya existe) podría ser engañada por dos threads que lean "no existe"
al mismo tiempo antes de que cualquiera inserte.

---

# PASO 3 — transfer-api (lógica completa)

---

## `entity/Transaction.java`

**¿Qué es?**
Clase Java que mapea la tabla `transactions` de MySQL como un objeto Java que JPA puede
leer, insertar y actualizar automáticamente sin escribir SQL.

**¿Por qué `@Entity` en lugar de consultas SQL directas?**
Con JPA/Hibernate, en lugar de escribir `"INSERT INTO transactions VALUES (...)"` en
cada servicio, trabajas con objetos Java. Hibernate genera el SQL internamente y lo
adapta al dialecto de cada base de datos. Además, Spring Data JPA genera el repositorio
completo sin que escribas ninguna implementación.

**¿Por qué `@Table(name = "transactions")`?**
Por convención, Hibernate usaría el nombre de la clase como nombre de tabla. Como la clase
se llama `Transaction` (singular), Hibernate buscaría la tabla `transaction`. Nuestra
tabla se llama `transactions` (plural). `@Table` resuelve el mismatch explícitamente.

**Detalle de cada campo y anotación:**

```java
@Id
@Column(name = "id", updatable = false, length = 36)
private String id;
// @Id → este campo es la clave primaria de la tabla
// updatable = false → Hibernate nunca incluye este campo en un UPDATE
//   (la PK nunca cambia una vez insertada)
// length = 36 → longitud máxima del VARCHAR en la DB (UUID tiene 36 chars)

@Enumerated(EnumType.STRING)
@Column(name = "status", length = 30)
private TransferStatus status;
// @Enumerated(STRING) → guarda "PROCESSING" en lugar del índice 0
// Sin esta anotación, Hibernate usa ORDINAL por defecto:
//   guarda 0 para PROCESSING, 1 para VALIDATED, etc.
// Si alguien agrega un valor nuevo al enum en medio de la lista,
//   todos los registros existentes apuntan al valor incorrecto.
// Con STRING eso no puede pasar.

@Builder.Default
private TransferStatus status = TransferStatus.PROCESSING;
// @Builder.Default → cuando usas el builder, si no especificas status,
//   el valor por defecto es PROCESSING.
//   Sin esto, @Builder ignora los valores default de Java y pone null.

@CreationTimestamp
@Column(name = "created_at", updatable = false)
private Instant createdAt;
// @CreationTimestamp → Hibernate asigna Instant.now() automáticamente
//   en el primer INSERT. updatable=false lo bloquea en UPDATEs posteriores.

@UpdateTimestamp
@Column(name = "updated_at")
private Instant updatedAt;
// @UpdateTimestamp → Hibernate reasigna Instant.now() en cada UPDATE.
//   Equivalente al ON UPDATE CURRENT_TIMESTAMP del SQL pero en Java.
```

---

## `dto/TransferRequest.java`

**¿Qué es?**
DTO (Data Transfer Object) que representa los datos que el cliente envía en el body del
POST /transfers. Es diferente de la entidad — solo contiene lo que el cliente puede enviar.

**¿Por qué no usar `Transaction` directamente como body del request?**
`Transaction` tiene campos que el cliente no controla: `id` (UUID generado internamente),
`status` (siempre empieza en PROCESSING), `failureReason`, `createdAt`, `updatedAt`. Si
usaras la entidad como request, un cliente malicioso podría enviar `"status": "COMPLETED"`
y saltarse el Saga por completo. El DTO expone solo los 3 campos que el cliente tiene
permitido enviar: `fromAccount`, `toAccount`, `amount`.

**¿Qué hace `@NotBlank` vs `@NotNull`?**
```java
@NotNull   → rechaza null, pero acepta "" (cadena vacía) y "   " (solo espacios)
@NotBlank  → rechaza null, "", y "   " — es la validación correcta para Strings
```

**¿Qué hace `@Positive` en `amount`?**
```java
@Positive  → rechaza 0 y cualquier valor negativo, acepta cualquier positivo
@Min(1)    → rechaza valores menores a 1 (funciona con enteros, no con BigDecimal)
// @Positive es la anotación correcta para BigDecimal monetario
```

---

## `dto/TransferResponse.java`

**¿Qué es?**
DTO de respuesta para POST /transfers y GET /transfers/{id}. Contiene más información
que el request porque el servidor agrega datos (transactionId, status, timestamps).

**`@JsonInclude(JsonInclude.Include.NON_NULL)`**
Jackson omite del JSON los campos que son `null`. Resultado:
```json
// Transferencia exitosa (sin failureReason):
{ "transactionId": "...", "status": "COMPLETED" }

// Sin @JsonInclude:
{ "transactionId": "...", "status": "COMPLETED", "failureReason": null }
```
La primera respuesta es más limpia. El cliente no necesita saber que un campo es null —
simplemente no está.

**`@JsonFormat(shape = JsonFormat.Shape.STRING)` en `Instant`**
Sin esta anotación, Jackson serializa un `Instant` como array de números: `[2024,1,15,...]`.
Con ella, lo serializa como ISO-8601: `"2024-01-15T10:30:00Z"`. Legible para humanos
y parseble por cualquier lenguaje.

---

## `repository/TransactionRepository.java`

**¿Qué es?**
Interfaz que le dice a Spring Data JPA qué operaciones DB necesita este servicio.
Spring genera la implementación completa en tiempo de arranque — no hay ningún método
que implementar manualmente.

**¿Cómo funciona `JpaRepository<Transaction, String>`?**
Al extender esta interfaz declaras:
- `Transaction` → el tipo de entidad que maneja
- `String` → el tipo del campo `@Id` (el `id` de Transaction es String)

Spring Data JPA genera automáticamente: `save()`, `findById()`, `findAll()`, `delete()`,
`count()`, `existsById()`, y más.

**¿Cómo funciona `findByStatus(TransferStatus status)`?**
Spring Data JPA lee el nombre del método y genera SQL automáticamente:
```
findByStatus → SELECT * FROM transactions WHERE status = ?
```
El parámetro `TransferStatus status` se convierte en el valor del WHERE.
No hay SQL, no hay anotaciones extra — solo la convención de nombres.

---

## `producer/TransferEventProducer.java`

**¿Qué es?**
Componente responsable de publicar eventos en Kafka. Es el único lugar del servicio
que conoce cómo hablarle a Kafka.

**¿Por qué `KafkaTemplate<String, Object>` y no `KafkaTemplate<String, TransferRequestedEvent>`?**
Un producer puede necesitar enviar distintos tipos de eventos en el futuro. Con `Object`
y `JsonSerializer`, Kafka serializa cualquier clase Java a JSON. Si en el futuro
`transfer-api` necesita publicar otro tipo de evento, este mismo template lo maneja.

**Método `publishTransferRequested(TransferRequestedEvent event)`:**
```java
kafkaTemplate.send(
    KafkaTopics.TRANSFER_REQUESTED,  // topic destino
    event.getFromAccount(),           // KEY de partición
    event                             // VALUE serializado a JSON
)
```
- `KafkaTopics.TRANSFER_REQUESTED` → constante `"transfer.requested"`. Nunca un String
  literal directo — si el nombre cambia, lo cambias en un solo lugar.
- `event.getFromAccount()` como KEY → Kafka garantiza que todos los mensajes con la misma
  key van a la misma partición en el mismo orden. Todas las transferencias de `ACC-001`
  van a la partición X y se procesan secuencialmente.

**El callback `whenComplete((result, ex) -> {...})`:**
`send()` es asíncrono — retorna un `CompletableFuture` inmediatamente sin esperar al broker.
El callback se ejecuta más tarde cuando el broker confirma (`result != null`) o rechaza
(`ex != null`). Esto libera el hilo HTTP del controller de esperar la confirmación de Kafka
(que puede tardar 10-100ms). El controller responde al cliente inmediatamente.

---

## `service/TransferService.java`

**¿Qué es?**
La capa de lógica de negocio. Orquesta las operaciones DB y Kafka. El controller le delega
todo — el controller solo se encarga de HTTP.

**Método `initiateTransfer(TransferRequest request)`:**

```java
@Transactional
public TransferResponse initiateTransfer(TransferRequest request) {
    // 1. Generar UUID
    String transactionId = UUID.randomUUID().toString();
    // UUID = Universally Unique Identifier. Con 2^122 posibles valores,
    // la probabilidad de colisión es despreciable.
    // Se genera en Java (no en DB) para conocer el ID ANTES del INSERT
    // y poder incluirlo en el evento Kafka.

    // 2. Crear y persistir la entidad
    Transaction transaction = Transaction.builder()
            .id(transactionId)
            .fromAccount(request.getFromAccount())
            .toAccount(request.getToAccount())
            .amount(request.getAmount())
            .status(TransferStatus.PROCESSING)
            .build();
    transactionRepository.save(transaction);
    // save() → Hibernate genera: INSERT INTO transactions VALUES (?,?,?,?,?,...)
    // Si la DB está caída → DataAccessException → @Transactional hace rollback
    // El evento Kafka NO se publica porque la excepción escala antes de llegar al producer

    // 3 y 4. Construir y publicar el evento
    TransferRequestedEvent event = TransferRequestedEvent.builder()
            .transactionId(transactionId)
            // ... campos del request ...
            .timestamp(Instant.now())
            .build();
    eventProducer.publishTransferRequested(event);
    // ORDEN CRÍTICO: DB primero, Kafka segundo.
    // Si Kafka falla, la transacción existe en DB (recuperable).
    // Si hiciéramos Kafka primero y DB falla, validation-service procesaría
    // un evento de una transacción que no existe en DB → error en cascada.

    // 5. Construir y devolver la respuesta HTTP
    return TransferResponse.builder()
            .status(TransferStatus.PROCESSING)
            // ...
            .build();
}
```

**Método `getStatus(String transactionId)`:**
```java
@Transactional(readOnly = true)
public TransferResponse getStatus(String transactionId) {
    Transaction t = transactionRepository.findById(transactionId)
            .orElseThrow(() -> new TransactionNotFoundException(...));
    // findById() → SELECT * FROM transactions WHERE id = ?
    // Optional.orElseThrow() → si el Optional está vacío (no encontró la fila),
    //   lanza la excepción. El controller la captura con @ExceptionHandler → HTTP 404
    return TransferResponse.builder()...build();
}
// readOnly = true → optimización de Hibernate: omite el dirty-checking
// (el proceso de comparar entidades con su estado inicial para detectar cambios).
// En una operación de solo lectura ese proceso es innecesario y costoso.
```

**Clase interna `TransactionNotFoundException`:**
```java
public static class TransactionNotFoundException extends RuntimeException { ... }
// static → no necesita una instancia de TransferService para crearse
// Extiende RuntimeException → no checked (no hay que declararla con throws)
// Vive dentro de TransferService porque solo tiene sentido aquí
// El controller la captura con @ExceptionHandler(TransferService.TransactionNotFoundException.class)
```

**`buildStatusMessage(TransferStatus status)` — switch expression (Java 14+):**
```java
return switch (status) {
    case PROCESSING  -> "En proceso: esperando validación";
    case COMPLETED   -> "✅ Transferencia completada exitosamente";
    // ...
};
// El switch expression de Java (con →) es exhaustivo: si agregas un nuevo
// valor al enum TransferStatus y no lo manejas aquí, el compilador da error.
// Es más seguro que if/else o switch clásico donde puedes olvidar un caso.
```

---

## `controller/TransferController.java`

**¿Qué es?**
Capa HTTP del servicio. Solo recibe requests, delega al service y construye responses.
No tiene lógica de negocio — si lo tuviera, sería difícil reutilizarla en otros contextos.

**`@RestController` = `@Controller` + `@ResponseBody`:**
- `@Controller` → Spring registra esta clase como handler de requests HTTP
- `@ResponseBody` → el retorno de cada método se serializa como JSON automáticamente
  en el body de la respuesta. Sin él, Spring intentaría resolver el String como
  nombre de una vista (Thymeleaf, JSP), lo que fallaría.

**`@RequestMapping("/transfers")`:**
Define el prefijo de URL para todos los métodos de este controller. Evita repetir
`/transfers` en cada `@GetMapping` y `@PostMapping`.

**`@PostMapping` — método `createTransfer`:**
```java
@PostMapping
public ResponseEntity<TransferResponse> createTransfer(
        @Valid @RequestBody TransferRequest request) {
// @Valid → activa Bean Validation sobre TransferRequest.
//   Si falla → Spring lanza MethodArgumentNotValidException ANTES de ejecutar el método
//   El @ExceptionHandler de abajo la captura → HTTP 400 con detalles

// @RequestBody → Jackson deserializa el JSON del body HTTP a TransferRequest
//   Sin @RequestBody, Spring intentaría leer los parámetros de la URL (?param=valor)

    return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    // HTTP 202 ACCEPTED: el recurso fue aceptado pero el procesamiento es asíncrono.
    // HTTP 201 CREATED significaría que la transferencia está completa y lista.
    // Aquí solo está PROCESSING — el Saga continúa en otros servicios.
}
```

**`@GetMapping("/{id}")` — método `getStatus`:**
```java
@GetMapping("/{id}")
public ResponseEntity<TransferResponse> getStatus(@PathVariable String id) {
// @PathVariable → extrae el valor de {id} de la URL: GET /transfers/uuid-aqui
//   El nombre de la variable ({id}) coincide con el parámetro (String id)
    return ResponseEntity.ok(response);  // HTTP 200
}
```

**`@ExceptionHandler(TransactionNotFoundException.class)`:**
```java
@ExceptionHandler(TransferService.TransactionNotFoundException.class)
public ResponseEntity<Map<String, String>> handleNotFound(...) {
// Spring llama a este método cuando CUALQUIER método del controller lanza
// TransactionNotFoundException. Convierte la excepción en HTTP 404 con JSON.
// Sin este handler, Spring devolvería un HTML de error por defecto.
    return ResponseEntity.status(NOT_FOUND).body(Map.of("error", ex.getMessage()));
}
```

**`@ExceptionHandler(MethodArgumentNotValidException.class)`:**
```java
// Se ejecuta cuando @Valid falla en algún campo del request.
// Recolecta TODOS los errores de validación (puede haber varios campos inválidos)
// y los devuelve en un mapa: { "campo": "mensaje de error" }
ex.getBindingResult().getFieldErrors().forEach(fieldError ->
    errors.put(fieldError.getField(), fieldError.getDefaultMessage())
);
// Ejemplo de respuesta:
// { "amount": "El monto debe ser mayor que cero",
//   "fromAccount": "La cuenta origen no puede estar vacía" }
```

---

---

# PASO 4 — validation-service

El validation-service es el primer consumer de la Saga. Su única responsabilidad: decidir si una
transferencia puede continuar o debe rechazarse. No mueve dinero. No actualiza estados. Solo valida
y publica el resultado. Esta separación de responsabilidades es lo que hace al servicio reemplazable
y testeable de forma independiente.

---

## `entity/Account.java`

**¿Qué es?**
Una entidad JPA que representa una fila de la tabla `accounts`. En este servicio la usamos
exclusivamente para preguntar "¿existe esta cuenta?" — nunca para modificar datos.

**¿Por qué existe?**
Spring Data JPA necesita una clase `@Entity` para poder ejecutar consultas sobre una tabla.
Sin esta clase, `AccountRepository` no tendría nada que mapear y el compilador fallaría.

**¿Por qué no tiene `@Builder`?**
`@Builder` genera un método estático `Account.builder()`. Si nunca vamos a construir un objeto
`Account` en este servicio (solo lo leemos de la DB), agregar `@Builder` sería ruido. Peor: podría
confundir a alguien que lea el código y piense que el servicio crea cuentas. La ausencia de
`@Builder` es una señal explícita de "este objeto solo se lee, nunca se construye aquí".

**¿Por qué tiene el campo `version Long`?**
Porque Flyway creó la columna `version` en la tabla `accounts` (la usa account-service para
Optimistic Locking). Hibernate en modo `ddl-auto: validate` compara campo por campo la clase
Java contra la tabla real. Si la tabla tiene `version` pero la clase no, Hibernate lanza:
`SchemaValidationException: missing column: version in accounts`. El campo existe en la clase solo
para satisfacer la validación — este servicio nunca lo usa.

**¿Por qué va en `entity/` antes que `repository/` y `service/`?**
Spring necesita registrar todos los beans en un orden que respeta las dependencias. `AccountRepository`
depende de `Account`. Si `Account` no existe, Spring falla al crear el contexto. La regla es:
primero la clase que el repositorio mapea, luego el repositorio, luego el servicio que lo inyecta.

---

## `entity/ProcessedEvent.java`

**¿Qué es?**
El registro de idempotencia. Cada vez que este servicio procesa un evento por primera vez, inserta
una fila en `processed_events`. La próxima vez que llegue el mismo evento (duplicado), lo detecta
aquí y lo descarta.

**¿Por qué `@GeneratedValue(strategy = GenerationType.IDENTITY)`?**
El ID de esta tabla es un contador auto-incremental que gestiona MySQL. No nos importa el valor
del ID — nunca lo buscamos por ID, siempre por `(event_key, service_name)`. Dejar que la DB lo
gestione elimina complejidad: no necesitamos UUID, no necesitamos sequence, no hay coordinación.

**El campo `serviceName` — ¿por qué existe si siempre es `"validation-service"`?**
Porque la tabla `processed_events` es **compartida** entre servicios en la misma DB. Si
account-service también registra eventos ahí, necesita una forma de distinguir "este evento lo
procesé yo" de "este evento lo procesó validation-service". El `UNIQUE(event_key, service_name)`
en la DB garantiza que:
- `("tx-123:TRANSFER_REQUESTED", "validation-service")` → solo un registro
- `("tx-123:TRANSFER_VALIDATED", "account-service")` → otro registro distinto

Sin `service_name`, los servicios colisionarían entre sí.

**¿Por qué `@CreationTimestamp`?**
Para auditoría: si hay un bug y un evento se procesa dos veces, el timestamp nos dice cuándo
ocurrió la primera vez. Hibernate lo rellena automáticamente al hacer el INSERT, sin que el código
tenga que hacerlo.

---

## `repository/AccountRepository.java`

**¿Qué es?**
Una interfaz que extiende `JpaRepository<Account, String>`. Spring Data JPA la implementa en tiempo
de ejecución — sin que escribamos ni una línea de SQL ni de código de acceso a datos.

**¿Por qué no tiene métodos declarados?**
Porque solo necesitamos `existsById(String id)`, que está heredado de `JpaRepository`. Spring Data
genera automáticamente:
```sql
SELECT EXISTS(SELECT 1 FROM accounts WHERE id = ?)
```
Esta query es más eficiente que `findById()` porque no transfiere ninguna columna — solo devuelve
0 o 1. Para preguntar "¿existe?", no necesitamos los datos de la fila.

**¿Por qué `JpaRepository<Account, String>` y no `CrudRepository`?**
`JpaRepository` extiende `CrudRepository` y agrega métodos como `findAll(Pageable)`,
`saveAllAndFlush()`. No los usamos, pero heredar de `JpaRepository` es la convención estándar
en proyectos Spring Boot y da más flexibilidad para el futuro sin costo alguno ahora.

---

## `repository/ProcessedEventRepository.java`

**¿Qué es?**
Interfaz JPA para operaciones sobre la tabla `processed_events`.

**El método `existsByEventKeyAndServiceName()`:**
```java
boolean existsByEventKeyAndServiceName(String eventKey, String serviceName);
```
Spring Data JPA lee el nombre del método y genera automáticamente:
```sql
SELECT EXISTS(SELECT 1 FROM processed_events
              WHERE event_key = ? AND service_name = ?)
```
No escribimos SQL. El nombre del método es el contrato. Si el nombre cambia, la query cambia.
Si hay un typo en el nombre, Spring falla al arrancar con un error descriptivo — detectamos el
bug en boot time, no en runtime.

**¿Por qué `boolean` y no `Optional<ProcessedEvent>`?**
Solo necesitamos saber si existe — no necesitamos los datos del registro. `boolean` transmite
la intención con exactitud y evita la creación innecesaria de un objeto en heap.

---

## `service/IdempotencyService.java`

**¿Qué es?**
El guardián contra procesamiento duplicado. Implementa dos de las tres capas de idempotencia del
sistema.

**Las 3 capas y por qué necesitamos las 3:**

```
CAPA 1 — Kafka producer (transfer-api)
  enable.idempotence=true
  El broker asigna un número de secuencia a cada mensaje.
  Si el producer reintenta por fallo de red, el broker descarta el duplicado.
  ✅ Protege contra: duplicados a nivel de red antes de que lleguen al topic.
  ❌ No protege contra: el mismo mensaje enviado dos veces desde código diferente.

CAPA 2 — SELECT EXISTS antes del INSERT
  if (processedEventRepository.existsByEventKeyAndServiceName(key, SERVICE_NAME)) return false;
  ✅ Protege contra: la mayoría de duplicados en condiciones normales.
  ❌ No protege contra: dos threads que lean "no existe" al mismo tiempo (race condition).

CAPA 3 — UNIQUE constraint en DB
  CONSTRAINT uq_processed_event UNIQUE (event_key, service_name)
  Si dos threads pasan la Capa 2 simultáneamente, solo uno puede hacer el INSERT.
  El segundo recibe DataIntegrityViolationException → lo manejamos como "ya procesado".
  ✅ Protege contra: condiciones de carrera entre threads concurrentes.
  ✅ Garantía absoluta: la DB es el árbitro final.
```

**`@Transactional(propagation = Propagation.REQUIRES_NEW)` — la decisión más importante:**

Sin este atributo, `tryRegister()` participaría en la transacción del caller (el consumer).
Si el consumer falla después del registro pero antes del ACK, el rollback de la transacción del
caller también revertiría el INSERT en `processed_events`. En el siguiente reintento, Kafka
reentrega el mensaje, la Capa 2 no ve registro y vuelve a procesarlo → doble procesamiento.

Con `REQUIRES_NEW`, `tryRegister()` abre su propia transacción independiente. El INSERT se
commitea antes de que el caller termine. Si el caller falla después, el registro persiste.
El reintento lo detecta como duplicado y lo descarta.

**`@Service` (no `@Component`) — ¿por qué?**
`@Service` es una especialización de `@Component` con significado semántico: "este bean contiene
lógica de negocio". Spring Boot los trata igual a nivel técnico, pero `@Service` documenta la
intención y herramientas como Spring Security pueden darle tratamiento especial en el futuro.

---

## `service/ValidationService.java`

**¿Qué es?**
El corazón del servicio: aplica 4 reglas de negocio en orden y publica el resultado.

**Patrón fail-fast — ¿qué es y por qué?**
Fail-fast significa "al primer problema detectado, parar y reportar". La alternativa sería
acumular todos los errores y reportarlos al final. En un sistema de transferencias:
- Un evento de `TRANSFER_FAILED` es suficiente para cancelar la saga
- Si publicáramos múltiples `TRANSFER_FAILED` para la misma transferencia, los consumers
  downstream tendrían que manejar múltiples fallos para la misma transacción — complejidad innecesaria
- El primer error ya es suficiente para que el usuario entienda qué corregir

**Las 4 reglas y por qué en ese orden:**

```
Regla 1: fromAccount ≠ toAccount
  → Comparación de String en memoria. Costo: nanosegundos.
  → Si ponemos esto al final, habremos hecho 2 queries a la DB innecesariamente.

Regla 2: amount > 0
  → Comparación de BigDecimal en memoria. Costo: nanosegundos.
  → transfer-api ya validó esto con @Positive, pero un producer mal configurado
    podría enviar amount=0 directamente al topic. Defensa en profundidad.

Regla 3: fromAccount existe en DB
  → 1 query SELECT EXISTS. Costo: ~1-5ms.
  → Va antes que la Regla 4 porque tiene más probabilidad de fallar: si alguien
    envía una cuenta origen inválida, sabemos que la Regla 4 tampoco sirve de nada.

Regla 4: toAccount existe en DB
  → 1 query SELECT EXISTS. Costo: ~1-5ms.
  → Va al final porque si las 3 reglas anteriores pasan, aquí hay alta probabilidad
    de pasar también (la cuenta destino generalmente existe).
```

**¿Por qué NO valida el saldo aquí?**
Porque entre la validación y el débito real (en account-service) puede pasar tiempo, y el saldo
puede cambiar. Si validamos "saldo suficiente" aquí pero para cuando account-service hace el débito
el saldo ya bajó por otra transferencia concurrente, tendríamos:
- validation-service publicó VALIDATED ✓
- account-service intenta debitar y no hay saldo suficiente → debe publicar FAILED
- La saga ya esperaba VALIDATED — ahora tiene que manejar un FAILED inesperado

account-service resuelve esto con `@Version` (Optimistic Locking): en el momento exacto del
débito verifica el saldo y si cambió, falla de forma controlada. Es el único servicio que puede
tener certeza sobre el saldo porque hace la modificación atómica.

---

## `producer/ValidationEventProducer.java`

**¿Qué es?**
El componente que escribe en Kafka. Traduce el resultado de la validación en eventos Kafka.

**`KafkaTemplate<String, Object>` — ¿por qué `Object` como tipo del valor?**
El producer publica dos tipos de eventos distintos: `TransferValidatedEvent` y `TransferFailedEvent`.
Si usáramos un tipo genérico específico (`KafkaTemplate<String, TransferValidatedEvent>`), no
podríamos publicar `TransferFailedEvent` con el mismo template. Usar `Object` permite publicar
cualquier clase serializable. El `JsonSerializer` se encarga de convertirlo a JSON en runtime.

**`fromAccount` como clave de partición:**
```java
kafkaTemplate.send(topic, original.getFromAccount(), event)
```
Kafka distribuye los mensajes entre particiones usando el hash de la clave. Usando `fromAccount`
como clave:
- Todos los eventos de la cuenta `ACC-001` van a la misma partición
- Kafka garantiza orden dentro de una partición
- Si `ACC-001` tiene 3 transferencias en paralelo, sus eventos llegan en orden al siguiente servicio

Si usáramos un UUID aleatorio como clave (o sin clave), los eventos de la misma cuenta podrían
llegar en diferente orden — condition-sensitive bug difícil de reproducir.

**`.whenComplete()` — ¿por qué async y no blocking?**
`kafkaTemplate.send()` es asíncrono: retorna un `CompletableFuture` inmediatamente sin esperar
confirmación del broker. `whenComplete()` registra un callback para cuando llegue la confirmación.
Si esperáramos de forma blocking, el thread del consumer quedaría bloqueado hasta que el broker
confirme la escritura — latencia innecesaria. El callback solo loggea; si falla, el consumer
ya hizo ACK (en caso de éxito del procesamiento).

---

## `consumer/TransferRequestedConsumer.java`

**¿Qué es?**
El punto de entrada del servicio. Recibe mensajes de Kafka y orquesta todos los componentes.

**`@KafkaListener` — los atributos más importantes:**
```java
@KafkaListener(
    topics  = KafkaTopics.TRANSFER_REQUESTED,  // constante → typos detectados en compilación
    groupId = "validation-group"               // duplicado del application.yml por claridad
)
```
El `groupId` también está en `application.yml`. ¿Por qué repetirlo aquí? Porque `@KafkaListener`
puede sobrescribir el group-id del `application.yml` para un listener específico. Ponerlo explícito
aquí hace que el código sea autoexplicativo: quien lea este método sabe a qué grupo pertenece sin
tener que buscar en el YAML.

**`Acknowledgment` — ¿cómo llega aquí?**
Spring Kafka inyecta el objeto `Acknowledgment` automáticamente en el método cuando `ack-mode:
MANUAL_IMMEDIATE` está configurado. No hay `@Autowired`. Spring sabe que si un parámetro del
método listener tiene tipo `Acknowledgment`, debe inyectarlo.

**La regla del ACK:**

```
¿Cuándo hacer ACK?              ¿Por qué?
──────────────────              ─────────────────────────────────────────────
Duplicado detectado             Sin ACK → Kafka reentrega → loop de duplicados
Procesamiento exitoso           Sin ACK → Kafka reentrega → procesamiento doble
Excepción inesperada            CON ACK → Kafka no reentrega → se pierde el mensaje
                                SIN ACK → Kafka reentrega → servicio puede recuperarse
```

La regla es: **ACK cuando sabemos que ya no necesitamos el mensaje; SIN ACK cuando queremos
que Kafka lo reintente.**

**¿Qué pasa si el servicio cae entre el `validationService.validate()` y el `acknowledgment.acknowledge()`?**
Kafka reentrega el mensaje al reiniciar. `idempotencyService.tryRegister()` detecta que ya está
en `processed_events` y retorna `false`. El consumer hace ACK inmediato. El evento de validación
ya fue publicado en Kafka en el intento anterior — no se vuelve a publicar. La saga continúa.

Esta es la garantía *at-least-once delivery* de Kafka combinada con idempotencia: aunque el
mensaje llegue más de una vez, el procesamiento ocurre exactamente una vez.

---

---

# PASO 5 — account-service

account-service es el único servicio del sistema que mueve dinero real. Todos los demás
coordinan, validan o registran — este es el que modifica balances. Esta responsabilidad única
justifica que sea el servicio más complejo y el que tiene más mecanismos de seguridad.

---

## `entity/Account.java`

**¿Qué es?**
La entidad central del sistema. Representa una fila de la tabla `accounts` con su saldo y versión.

**¿Por qué tiene `@Version` y qué hace exactamente?**

`@Version` es la anotación que activa el Optimistic Locking en JPA. Sin ella, JPA generaría:
```sql
UPDATE accounts SET balance = 900.00 WHERE id = 'ACC-001'
```

Con ella, JPA genera automáticamente:
```sql
UPDATE accounts SET balance = 900.00, version = version + 1
WHERE id = 'ACC-001' AND version = 5
```

Si el WHERE no encuentra la fila (porque otro thread ya actualizó la versión), JPA lanza
`ObjectOptimisticLockingFailureException`. El developer nunca tiene que escribir el `AND version=?` —
JPA lo hace por cada UPDATE de una entidad que tenga `@Version`.

**¿Por qué Optimistic y no Pessimistic Locking?**

Pessimistic Locking (`SELECT FOR UPDATE`) bloquea la fila en la DB hasta que la transacción
termine. Si el servicio muere con el lock activo, la DB espera el timeout antes de liberar la fila.
En un sistema con miles de transferencias por segundo, esto es un cuello de botella severo.

Optimistic Locking no bloquea nada. Asume que los conflictos son raros y los detecta solo
en el momento de escribir. Para cuentas bancarias (baja probabilidad de dos débitos simultáneos
de la misma cuenta), Optimistic es la elección correcta.

**¿Por qué `@NoArgsConstructor` pero no `@Builder`?**
Hibernate usa reflexión para crear instancias de la entidad al mapear resultados de queries.
Necesita el constructor sin argumentos. `@Builder` no tiene sentido porque nunca creamos
cuentas en código — las crea el script `init-db.sql`. Agregar `@Builder` daría una falsa
impresión de que el servicio crea cuentas.

---

## `entity/ProcessedEvent.java`

Idéntica a la de validation-service en estructura. La diferencia está en `SERVICE_NAME =
"account-service"` en IdempotencyService.

**¿Por qué repetir esta clase en lugar de ponerla en common?**
`@Entity` en `common` causaría que Spring Boot de cada servicio intente escanearla y mapearla
a una tabla. Si dos servicios apuntan a la misma DB (como en este lab), no hay problema. Pero
si apuntan a DBs distintas (escenario real), Hibernate intentaría crear la tabla en ambas DB
con potencialmente distintas versiones del schema. Más complejidad que beneficio — mejor
tener una copia controlada en cada servicio.

---

## `repository/AccountRepository.java`

**¿Por qué no tiene `@Lock(LockModeType.PESSIMISTIC_WRITE)`?**
Agregar este lock al `findById()` convertiría cada lectura de cuenta en un `SELECT FOR UPDATE`.
Funcionaría, pero:
1. Solo un thread puede leer la fila a la vez → el throughput cae dramáticamente
2. Si el servicio muere con el lock activo, la fila queda bloqueada hasta timeout de la DB
3. Kafka con múltiples particiones + múltiples instancias del servicio → deadlocks posibles

`@Version` logra la misma seguridad sin ninguno de estos problemas.

---

## `service/IdempotencyService.java`

Estructuralmente idéntica a la de validation-service. El único cambio es:
```java
private static final String SERVICE_NAME = "account-service";
```

Esto garantiza que los registros de idempotencia de este servicio no colisionan con los de
validation-service en la tabla compartida `processed_events`.

**Claves usadas por account-service:**
- `txId:TRANSFER_VALIDATED` — registrado por DebitConsumer antes de debitar
- `txId:TRANSFER_DEBITED` — registrado por CreditConsumer antes de acreditar
- `txId:COMPENSATION` — registrado por CompensationService al compensar

---

## `service/DebitService.java`

**Las 4 fases del método `debit()` con `@Transactional`:**

```
[BEGIN TRANSACTION]
  1. findById(fromAccount) → SELECT * FROM accounts WHERE id=?
     Obtiene: balance actual, version actual
  2. if (balance < amount) → throw InsufficientFundsException
     (defensa en profundidad — validation-service ya lo verificó)
  3. account.setBalance(balance - amount)
     Modifica el objeto en memoria, sin tocar la DB aún
  4. accountRepository.save(account)
     → UPDATE accounts SET balance=?, version=version+1 WHERE id=? AND version=<leído>
     Si 0 rows updated → JPA lanza ObjectOptimisticLockingFailureException
     Si 1 row updated → OK, retorna Account con nuevo balance
[COMMIT]
```

**¿Por qué `@Transactional` en el service y no en el consumer?**
El consumer hace ACK de Kafka y puede también publicar eventos en Kafka. Si el consumer fuera
`@Transactional`, la transacción de DB estaría abierta durante todo ese tiempo (incluyendo la
publicación en Kafka, que puede tardar). Mantener transacciones DB lo más cortas posible es
una buena práctica. Encapsular el `@Transactional` en el service hace que la transacción solo
dure lo que necesita: el READ, MODIFY, SAVE.

**Excepción `InsufficientFundsException` — ¿cuándo ocurre si validation-service ya verificó?**
Entre el momento en que validation-service verificó el saldo y el momento en que DebitService
lo descuenta, puede haber pasado tiempo (latencia de Kafka, colas de procesamiento, carga del
sistema). Durante ese tiempo, otra transferencia pudo haber debitado la cuenta. Es poco frecuente,
pero la defensa en profundidad garantiza que nunca debita más de lo disponible.

---

## `service/CreditService.java`

**El campo `simulateCreditFailure` inyectado con `@Value`:**
```java
@Value("${app.simulate-credit-failure:false}")
private boolean simulateCreditFailure;
```

`@Value` inyecta el valor desde application.yml en el momento en que Spring crea el bean.
El `:false` es el valor por defecto si la propiedad no está definida.

**¿Por qué simular el fallo ANTES de tocar la DB?**
La excepción se lanza al inicio del método, antes de `accountRepository.findById()`. Esto
garantiza que cuando el crédito "falla", la cuenta destino no fue modificada en absoluto.
El estado del sistema es: débito aplicado, crédito no aplicado. La compensación revierte
el débito, dejando el sistema en el estado inicial.

Si el fallo fuera a mitad del proceso (ej: después de findById pero antes del save), tendríamos
un estado inconsistente más difícil de limpiar.

---

## `service/CompensationService.java`

**¿Por qué la compensación tiene su propia idempotencia (`txId:COMPENSATION`)?**

Si CreditConsumer falla después de aplicar la compensación pero antes del ACK, Kafka reentrega
el mensaje `transfer.debited`. CreditConsumer volvería a intentar el crédito (falla por flag)
y llamaría a compensationService.compensate() de nuevo. Sin idempotencia en compensación,
aplicaría el crédito dos veces → `fromAccount` terminaría con más dinero del que tenía antes.

Con `txId:COMPENSATION` como clave de idempotencia, el segundo intento detecta que ya se
compensó y retorna null sin tocar el saldo.

**¿Por qué `@Transactional` directamente en este método (sin REQUIRES_NEW)?**
A diferencia de IdempotencyService (que necesita REQUIRES_NEW para que el registro persista
aunque el caller falle), CompensationService usa `@Transactional` normal porque:
1. El registro de compensación y el UPDATE del saldo deben ser atómicos (los dos o ninguno)
2. Si la compensación falla a mitad, queremos que el saldo NO haya cambiado
3. CreditConsumer capturará la excepción y la manejará (log de error crítico)

---

## `producer/AccountEventProducer.java`

**¿Por qué tiene 4 métodos con firmas diferentes en lugar de 1 genérico?**

Un único método `publish(Object event, String topic)` sería más corto, pero:
- Perderíamos el chequeo de tipos en compilación
- Alguien podría llamar `publish(debitedEvent, TRANSFER_CREDITED)` por error — el compilador
  no lo detectaría, pero el consumer downstream recibiría un tipo inesperado y rompería
- Con 4 métodos específicos, el contrato es explícito: `publishDebited()` solo acepta
  `TransferValidatedEvent` porque necesita los datos exactos de ese evento

**`remainingBalance` y `newBalance` — ¿por qué incluirlos en los eventos?**
Es el patrón Event-Carried State Transfer: el evento lleva todos los datos que los downstream
consumers podrían necesitar, evitando que tengan que hacer queries a la DB para obtenerlos.

- `transfer.debited` incluye `remainingBalance`: útil para status-service (puede mostrarlo)
- `transfer.credited` incluye `newBalance`: útil para status-service (puede mostrarlo)

Sin estos campos, status-service tendría que llamar al API de account-service para obtener
el saldo — introduciendo una dependencia sincrónica que rompería el aislamiento del Saga.

---

## `consumer/DebitConsumer.java`

**Las 5 rutas posibles del método `onTransferValidated()`:**

| Situación | Acción | ACK |
|-----------|--------|-----|
| Duplicado (idempotencia) | Log warn, retornar | ✅ |
| Débito exitoso | publishDebited | ✅ |
| Saldo insuficiente | publishFailed | ✅ (no reintentable) |
| Conflicto optimista (`@Version`) | Log warn | ❌ (Kafka reentrega) |
| Error inesperado (bug, DB caída) | Log error | ❌ (Kafka reentrega) |

**El tradeoff de OptimisticLockException + idempotencia:**
Cuando ocurre un conflicto optimista, la idempotencia ya registró el evento (REQUIRES_NEW
committéd). Kafka reentrega el mensaje, pero la idempotencia lo filtra como duplicado → ACK
sin reprocesar. Significa que el débito falló silenciosamente.

Para el lab: aceptable porque los conflictos son raros en un sistema de dev con pocos datos.
En producción: usar el Outbox Pattern (debit y registro de idempotencia en la misma transacción)
elimina este problema.

---

## `consumer/CreditConsumer.java`

**¿Por qué CreditConsumer escucha `transfer.debited` y no `transfer.validated`?**
Porque el crédito solo puede ocurrir si el débito fue exitoso. El evento `transfer.debited`
es la confirmación de que el dinero fue descontado de `fromAccount`. No tendría sentido acreditar
`toAccount` sin saber si el débito ocurrió — sería crear dinero de la nada.

**`groupId = "account-credit-group"` — distinto al de DebitConsumer:**
account-service publica `transfer.debited` (DebitConsumer) y también lo consume (CreditConsumer).
Si ambos usaran el mismo grupo (`account-debit-group`), Kafka los trataría como instancias
competidoras y repartiría los mensajes entre ellos — solo uno procesaría cada mensaje, pero
no necesariamente el correcto. Con grupos distintos sobre topics distintos, cada consumer
recibe independientemente todos los mensajes de su topic.

**`handleCreditFailure()` — método privado de orquestación:**
Encapsula el flujo de compensación para no duplicar código entre los dos catch blocks:
1. `SimulatedCreditFailureException` (simulado)
2. `AccountNotFoundException` (cuenta cerrada entre validación y crédito)
Ambos necesitan compensar y publicar failed → mismo método, distinta causa.

---

---

# PASO 6 — status-service

status-service es el único observador pasivo del Saga. No mueve dinero, no valida reglas, no
toma decisiones. Solo escucha lo que ocurrió y registra el resultado. Esta simplicidad es
intencional: hace que sea prácticamente imposible que status-service introduzca bugs de negocio.

---

## `entity/Transaction.java`

**¿Qué es?**
Entidad JPA que mapea la tabla `transactions`. Esta tabla fue creada por transfer-api y
status-service la comparte en el lab (misma MySQL).

**¿Por qué status-service tiene su propia copia de esta entidad y no la toma de common?**

Porque los módulos `common` solo deben contener contratos de comunicación entre servicios:
eventos, enums, constantes. Poner una entidad JPA en `common` significaría que `common`
dependería de `spring-boot-starter-data-jpa` — y esa dependencia se propagaría a todos los
servicios que usen `common`, incluyendo los que no tienen DB.

Peor aún: si `common` tuviera `@Entity Transaction`, Spring Boot de cada servicio la escanearia
e intentaría gestionar el schema de esa tabla. Dos servicios compitiendo por el mismo schema
es un caos en producción.

La regla: **`@Entity` nunca en `common`**. Cada servicio tiene su propia representación Java
de las tablas que necesita, aunque sean "la misma tabla".

**Diferencia con la entidad en transfer-api:**

| Campo | transfer-api | status-service |
|-------|-------------|----------------|
| `@Builder` | Sí (crea transacciones) | No (solo lee/actualiza) |
| `@Builder.Default` para status | Sí (PROCESSING) | No (Hibernate lo rellena desde DB) |
| Setter de status | Sí | Sí (StatusService lo usa) |

**¿Por qué `@Enumerated(EnumType.STRING)` y no `@Enumerated(EnumType.ORDINAL)`?**

`ORDINAL` almacena el número del enum: `PROCESSING=0`, `VALIDATED=1`, `DEBITED=2`, etc.
Si algún día agregas un valor en el medio del enum (ej: `QUEUED` entre `PROCESSING` y
`VALIDATED`), todos los ordinals posteriores cambian → los datos históricos en la DB se
corrompen silenciosamente.

`STRING` almacena `"PROCESSING"`, `"VALIDATED"`, etc. Puedes reordenar el enum sin riesgo.
El único costo es que ocupa más espacio en disco (texto vs entero), pero eso es aceptable
para auditabilidad y robustez.

---

## `repository/TransactionRepository.java`

**El método `findByStatus(TransferStatus status)` — ¿para qué sirve?**

No lo usa ningún consumer ni servicio en el flujo normal. Está para monitoring operacional:
```java
// ¿Cuántas transferencias llevan más de 5 minutos en PROCESSING?
// (posible síntoma de que validation-service está caído)
List<Transaction> stuck = repo.findByStatus(TransferStatus.PROCESSING);
```

En producción esto se conectaría a un sistema de alertas. Para el lab, es útil para
verificar el estado del sistema desde la consola de MySQL o desde un endpoint de admin.

**`JpaRepository<Transaction, String>` — ¿por qué `String` como tipo del ID?**
El ID de la transacción es un UUID en formato String (`"f47ac10b-58cc-..."`). Usar `String`
como tipo del ID en JpaRepository permite llamar `findById("f47ac10b-...")` directamente
sin conversiones.

---

## `service/StatusService.java`

**La verificación de idempotencia simple:**

```java
if (tx.getStatus() == newStatus) {
    log.debug("Estado ya en {} — no se actualiza", newStatus);
    return;  // no hacer el UPDATE innecesario
}
```

Esta verificación evita escrituras redundantes en DB. Si status-service recibe el mismo
evento dos veces (redelivery de Kafka), la segunda vez el estado ya está actualizado → no
hay que ejecutar otro UPDATE. Esto es más eficiente que siempre hacer save() y más claro
que añadir una tabla processed_events (las actualizaciones de estado son inherentemente
idempotentes — no hay efecto secundario en setear el mismo valor dos veces).

**El caso `opt.isEmpty()` — ¿cuándo ocurre?**

En el arranque con `auto.offset.reset: earliest`, status-service puede recibir eventos
de transferencias del pasado cuya fila en `transactions` fue eliminada (ej: por limpieza
manual). También puede ocurrir en una race condition durante el arranque simultáneo de
todos los servicios: transfer-api publica el evento antes de hacer commit a la DB
(muy raro con `@Transactional`, pero teóricamente posible).

Para el lab: loggeamos el warning y continuamos. Para producción: reintentar después de
un delay (el INSERT de transfer-api probablemente se commitó en ese tiempo).

**`@Transactional(readOnly = true)` en `findById()` — ¿qué aporta?**

Hints a JPA para optimizar la sesión: en modo readOnly, Hibernate no mantiene el snapshot
del objeto para detección de cambios. Para queries de solo lectura esto reduce el overhead
de memoria y puede mejorar el rendimiento en DB con replicación (la query puede ir al
réplica de lectura en lugar del primario).

---

## `consumer/SagaEventConsumer.java`

**Un único `@KafkaListener` para 5 topics — ventajas y desventajas:**

| Criterio | 1 listener multi-topic (elegido) | 5 listeners separados |
|----------|----------------------------------|-----------------------|
| Líneas de código | ~60 | ~120 |
| Type safety | Requiere instanceof | @Payload tipo concreto |
| Agregar topic nuevo | 1 línea en array + 1 instanceof | Nuevo método completo |
| Flujo de control | Centralizado, un lugar | Distribuido |
| Debugging | Un método para buscar en logs | Hay que buscar en 5 |
| Claridad | Alta (todo visible en un lugar) | Baja (lógica fragmentada) |

Para status-service, la opción de un único listener es claramente mejor: la lógica de
cada caso es trivial (una llamada a updateStatus) y no tiene sentido multiplicar boilerplate.

**`ConsumerRecord<String, Object>` vs `@Payload Object`:**

`ConsumerRecord` expone:
```java
record.topic()     // "transfer.validated", "transfer.debited", etc. → útil para logging
record.partition() // número de partición → útil para debugging de distribución
record.offset()    // posición en el topic → útil para rastrear mensajes específicos
record.key()       // fromAccount → la clave de partición
record.value()     // el evento deserializado → lo que nos importa para el negocio
```

Con `@Payload Object` solo tendríamos `record.value()`. En un consumer de monitoring
como este, el contexto adicional del `ConsumerRecord` vale el pequeño costo de verbosidad.

**¿Por qué el `else` final (tipo desconocido) hace ACK en lugar de no hacer ACK?**

Si no hacemos ACK para tipos desconocidos, Kafka reentregará el mensaje indefinidamente.
Como el tipo seguirá siendo desconocido en cada reintento, entraríamos en un loop infinito
que bloquea el procesamiento de mensajes posteriores de esa partición.

La decisión correcta: loggear el warning (para detectar el problema) y hacer ACK
(para no bloquear el Saga). En producción: enviar el mensaje a un DLQ (Dead Letter Queue)
para investigación posterior.

---

## `controller/StatusController.java`

**`Map<String, Object>` en lugar de un DTO — ¿cuándo es aceptable?**

Para el lab es aceptable porque:
- El endpoint es interno (no es una API pública con contrato formal)
- No hay clientes externos que dependan de este schema
- Evita crear una clase `StatusResponse` que sería idéntica en 80% a `TransferResponse` de transfer-api

En producción se usaría un DTO con:
- `@JsonInclude(NON_NULL)` → para no incluir `failureReason` cuando es null
- `@JsonFormat(shape=STRING)` → para serializar `Instant` como ISO-8601
- Javadoc del contrato → para quien consuma la API

**El método `buildStatusMessage()` con `switch` exhaustivo:**

```java
return switch (status) {
    case PROCESSING  -> "⏳ En proceso — esperando validación";
    case VALIDATED   -> "✔️ Validada — esperando débito";
    // ...
    case ROLLED_BACK -> "🔄 Débito revertido — dinero devuelto al origen";
};
```

Se usa `switch expression` (Java 14+) sin `default`. Al ser exhaustivo sobre un enum,
el compilador garantiza que si se agrega un nuevo valor al enum `TransferStatus`, este
switch no compilará hasta que se agregue el caso correspondiente. Es una forma de hacer
que los cambios en el enum sean visibles en tiempo de compilación, no en tiempo de ejecución.

---

# El sistema completo — integración de todos los servicios

Con PASO 6 completado, el Saga Choreography está completo. El flujo de extremo a extremo:

```
Cliente
  │ POST /transfers
  ▼
transfer-api (8080)
  │ INSERT transactions (status=PROCESSING)
  │ publish transfer.requested
  ▼
validation-service (8081) ← group: validation-group
  │ 4 reglas de negocio
  │ publish transfer.validated / transfer.failed
  ▼
account-service (8082) ← group: account-debit-group
  │ Débito con @Version (Optimistic Locking)
  │ publish transfer.debited
  ▼
account-service (8082) ← group: account-credit-group
  │ Crédito (o compensación si simulate-credit-failure=true)
  │ publish transfer.credited (o transfer.compensated + transfer.failed)
  │
  └─────────────────────────────────────────────────┐
                                                     │
status-service (8083) ← group: status-group          │
  │ Escucha validated/debited/credited/failed/compensated
  │ UPDATE transactions SET status=...
  ▼
Cliente
  │ GET /transfers/{id}        → transfer-api (8080)
  │ GET /transfers/{id}/status → status-service (8083)
  ▼
{ "status": "COMPLETED" }
```

Cada servicio tiene su propio grupo de consumers. Kafka garantiza que:
- Todos los servicios reciben todos los mensajes de sus topics (cada grupo recibe todos)
- Los mensajes de la misma cuenta van a la misma partición (orden garantizado por clave)
- Si un servicio cae, sus mensajes se acumulan y los procesa al reiniciar (durabilidad)

---

# PASO 7 — Decisiones del demo de idempotencia

## ¿Por qué offset reset en lugar de kafka-console-producer para simular duplicados?

La alternativa más obvia sería usar el `kafka-console-producer.sh` para publicar manualmente
un mensaje duplicado. El problema: el `JsonDeserializer` de Spring Kafka lee el header
`__TypeId__` (agregado por `JsonSerializer`) para determinar la clase destino. El
`kafka-console-producer.sh` no puede agregar headers en la mayoría de las versiones de
Kafka disponibles en el lab. Sin el header correcto, el consumer lanza `JsonParseException`
— que sería un error de infraestructura, no una demostración de idempotencia.

**El offset reset es más realista** además de más práctico: simula exactamente el escenario
de producción donde un servicio se reinicia después de un fallo sin haber confirmado sus
offsets. Es literalmente lo que Kafka haría — reenviar mensajes desde el último offset confirmado.

## ¿Por qué demostrar idempotencia en account-service y no en validation-service?

account-service tiene la consecuencia más visible si la idempotencia falla: el saldo de la
cuenta disminuye el doble. En validation-service, un duplicado publicaría el evento
`transfer.validated` dos veces — detectable, pero menos dramático para demostrar.

Mostrar ambos servicios (section 7.5) da cobertura completa al concepto.

## ¿Por qué `kafka-consumer-groups.sh --reset-offsets` requiere que el grupo esté inactivo?

Kafka no permite modificar offsets de un grupo mientras hay consumers activos porque:
1. Los consumers ya tienen los offsets "reservados" en memoria
2. El coordinador del grupo (broker) necesita que todos los members hayan abandonado
   antes de aceptar un reset manual — de lo contrario el cambio podría ignorarse

Por eso el procedimiento es: Ctrl+C → reset → restart.

---

# PASO 8 — Decisiones del demo de compensación

## ¿Por qué `simulate-credit-failure` como flag de configuración y no un endpoint?

Un endpoint `POST /simulate/fail-credit` sería más conveniente para el demo (sin reiniciar).
Pero introduce un endpoint de "backdoor" que en producción representaría un riesgo de seguridad.
Un flag de configuración en `application.yml` requiere un reinicio deliberado — eso hace más
explícito que estás modificando el comportamiento del servicio y reduce el riesgo de activarlo
accidentalmente en producción.

La anotación `@Value("${app.simulate-credit-failure:false}")` también documenta el valor por
defecto directamente en el código: quien lea `CreditService.java` ve inmediatamente que el
comportamiento normal es `false`.

## ¿Por qué la compensación no tiene su propio topic `transfer.rollback`?

En un Saga real con muchos servicios (inventario, notificaciones, pagos externos), la
compensación se coordina via eventos porque cada servicio necesita "deshacer" su parte.
Publicar `transfer.rollback` en Kafka y tener un `CompensationConsumer` en cada servicio
es el patrón correcto para ese escenario.

Para este lab con un solo servicio que modifica dinero (`account-service`), la compensación
in-process es suficiente y más simple. El tradeoff: si la JVM muere entre el `publishCompensated()`
y el `acknowledge()`, Kafka reentrega `transfer.debited` y el `CreditConsumer` intenta compensar
de nuevo. La idempotencia con clave `txId:COMPENSATION` lo protege.

## ¿Por qué el `@Version` del account también protege la compensación?

`CompensationService` llama `accountRepository.save(account)` igual que `DebitService`. JPA
genera el mismo `UPDATE ... WHERE version=N`. Si dos threads intentan compensar simultáneamente
(muy improbable, pero posible con Kafka retry), solo uno logra el UPDATE. El otro recibe
`OptimisticLockingFailureException`. Como `CompensationService` también registra su propia
clave de idempotencia (`txId:COMPENSATION`), el segundo intento la detecta y sale sin hacer nada.

---

# PASO 9 — Decisiones del cierre

## ¿Por qué el script de prueba integral (9.2) usa PowerShell nativo en lugar de un test JUnit?

Un test JUnit de integración requeriría:
- Levantar los 4 servicios (TestContainers o configuración manual)
- Coordinar timing entre servicios asíncronos (¿cuándo esperar para verificar?)
- Mocks o infraestructura real de Kafka y MySQL en el classpath de tests

Para el propósito del lab (demostrar el sistema funcionando), un script PowerShell que llama
los endpoints reales es más directo y requiere menos infraestructura de testing. El tiempo que
tomaría escribir tests de integración robustos superaría el valor pedagógico para este lab.

En un proyecto real: sí se escribirían tests de integración con `@SpringBootTest`, `EmbeddedKafka`
y `TestContainers` para MySQL. Eso sería el tema de un lab dedicado a testing.

## ¿Por qué las preguntas de reflexión están en LAB-GUIDE y no en DECISIONES?

Las preguntas de reflexión (sección 9.5) tienen respuestas cortas orientadas al estudiante.
DECISIONES está orientado a explicar el razonamiento de las decisiones de implementación en
profundidad. Son audiencias distintas: la reflexión es para el estudiante que acaba de hacer
el lab; DECISIONES es para quien quiere entender por qué el código es así mientras lo lee.

---

# PASO 10 — Decisiones de los Bonus

## Bonus A — Redis cache: ¿por qué un TTL diferenciado?

La decisión más importante del Bonus A es tener DOS valores de TTL en lugar de uno uniforme.

**El problema con un TTL uniforme largo (ej: 5 minutos):**
Si cacheamos `PROCESSING` durante 5 minutos, el cliente que hace polling verá
`PROCESSING` durante 5 minutos aunque el Saga completó en 3 segundos.
Esto rompería la experiencia del usuario: "¿por qué dice PROCESSING si ya pasaron 5 minutos?".

**El problema con un TTL uniforme corto (ej: 5 segundos):**
Funciona correctamente pero no da beneficio para estados finales.
`COMPLETED` se evicta de la caché cada 5 segundos. Si el cliente consulta
el estado 100 veces (ej: pantalla de historial), las 100 queries van a MySQL.
El punto de tener caché era reducir esa carga.

**La solución — TTL diferenciado:**
```
Estado INTERMEDIO (PROCESSING, VALIDATED, DEBITED, CREDITED) → 5 segundos
  Razón: el Saga sigue avanzando, la información cambia pronto.
  Con 5 segundos, el cliente ve el estado "correcto" con máximo 5 segundos de retraso.

Estado FINAL (COMPLETED, FAILED, ROLLED_BACK) → 10 minutos
  Razón: el estado NUNCA cambiará. Podemos cachearlo mucho tiempo.
  Con 10 minutos, 100 consultas al mismo estado final → 99 hits de Redis, 1 query MySQL.
```

**¿Por qué Redis y no Caffeine (caché en memoria)?**

Caffeine (caché local JVM) sería más simple de configurar, pero tiene un problema:
si `transfer-api` tiene múltiples instancias (horizontal scaling), cada instancia tiene
su propia caché local → inconsistencia entre instancias. La instancia A cachea COMPLETED,
la instancia B no lo tiene y va a MySQL, la instancia C tiene PROCESSING desactualizado.

Redis es un caché centralizado → todas las instancias comparten el mismo dato.

Para este lab con una sola instancia, Caffeine funcionaría igual. Elegimos Redis porque:
1. Demuestra el patrón que se usa en producción
2. Permite introspección con `redis-cli` (ver las keys, TTLs, valores)
3. Ya está en docker-compose (un servicio más no añade complejidad significativa)

## Bonus A — ¿Por qué configurar el RedisTemplate manualmente?

Spring Boot auto-configura `RedisTemplate<Object, Object>` con `JdkSerializationRedisSerializer`.
Eso serializa las keys como bytes Java (ilegibles) y los values como Java serializado (binario).

Problemas concretos:
1. `redis-cli KEYS *` mostraría: `\xac\xed\x00\x05t\x00\x11transfer:f47a...` → ilegible
2. Si cambias un campo de `TransferResponse` y hay datos viejos en Redis → deserialización falla
3. Los valores no son inspeccionables en redis-cli

Con `Jackson2JsonRedisSerializer` + `StringRedisSerializer`:
- Keys: `transfer:f47ac10b-58cc-...` (legible)
- Values: JSON completo (versionable, inspeccionable)

`Jackson2JsonRedisSerializer` (tipado a `TransferResponse`) vs `GenericJackson2JsonRedisSerializer`:
- El genérico incluye `"@class": "com.lab.api.dto.TransferResponse"` en el JSON
- Crea acoplamiento al nombre del paquete → si refactorizas el paquete, los datos viejos en Redis no deserializan
- El tipado no incluye metadatos de clase → JSON más limpio y desacoplado del nombre del paquete

## Bonus A — Tolerancia a fallos de Redis

Cada operación de Redis (`get`, `put`) está envuelta en `try/catch`.
Si Redis no está disponible (contenedor caído, red particionada):
- `get()` → retorna `Optional.empty()` → `TransferService` cae a MySQL
- `put()` → no hace nada → `TransferService` igual devuelve la respuesta

El sistema funciona aunque Redis esté caído. Solo se pierde el beneficio del cache.
En producción, se añadiría una métrica (`redis.miss.fallback`) para alertar cuando
el fallback ocurre con frecuencia.

## Bonus A — ¿Por qué `TransferResponse` necesitaba `@NoArgsConstructor`?

`TransferResponse` originalmente solo tenía `@Data @Builder`.
`@Builder` de Lombok genera un constructor privado all-args para el builder.
Jackson, al deserializar desde Redis (JSON → objeto Java), necesita:
1. Un constructor sin argumentos (para instanciar el objeto vacío)
2. Los setters (que `@Data` genera)

Sin `@NoArgsConstructor`, Jackson lanzaba:
`InvalidDefinitionException: No suitable constructor found for type [TransferResponse]`

Con `@NoArgsConstructor + @AllArgsConstructor`, Lombok genera ambos constructores
y `@Builder` sigue funcionando usando el all-args.

## Bonus B — ¿Por qué MDC en lugar de pasar el txId como parámetro?

**Alternativa A (sin MDC):** Pasar el `transactionId` a cada método que loguea.
```java
validationService.validate(event, event.getTransactionId()); // solo para logging
```
El txId contamina la firma de todos los métodos. No tiene nada que ver con la lógica
de negocio — es solo infraestructura de observabilidad. Viola el principio de separación.

**Alternativa B (MDC):** Poner el txId en MDC una sola vez al inicio del consumer.
Todos los logs producidos en el mismo hilo durante ese procesamiento (incluidos los
de ValidationService, DebitService, etc.) ven el txId automáticamente.
No cambia ninguna firma de método. Es transparente para la lógica de negocio.

**¿Por qué `finally { MDC.remove("txId") }`?**
Los hilos de KafkaListenerContainer son reutilizados entre mensajes (thread pool).
Si no limpiamos el MDC, el txId de la transferencia anterior aparecería en los logs
del mensaje siguiente. `finally` garantiza que la limpieza ocurre incluso si hay excepción.

## Bonus B — ¿Por qué la interfaz SagaEvent solo tiene dos métodos?

```java
public interface SagaEvent {
    String getTransactionId();
    String getFromAccount();
}
```

Solo tiene los campos necesarios para **observabilidad** (logging y tracing):
- `transactionId` → correlacionar todos los logs del mismo Saga
- `fromAccount` → filtrar logs por cliente en sistemas de logging centralizados

¿Por qué no incluir `amount`, `timestamp`, `toAccount`?
MDC es infraestructura de logging, no de negocio. Cada servicio ya tiene acceso
a esos campos via el evento concreto (`TransferValidatedEvent`, etc.).
Incluirlos en la interfaz los haría disponibles en MDC, pero ¿qué log los necesita
sin ya tener acceso al evento completo? Ninguno.

La interfaz mínima es menos acoplada. Si un evento futuro no tiene `amount`
(ej: un evento de configuración que también es parte de un Saga), no tiene
que proporcionar un valor falso.

## Bonus B — ¿Por qué `SagaEvent` en el módulo `common` y no en cada servicio?

La interfaz es compartida por:
- `common` → los 6 eventos la implementan
- `status-service` → `SagaEventConsumer` hace `event instanceof SagaEvent se`
- Cualquier servicio que en el futuro quiera extracción polimórfica de txId

Si estuviera en `status-service`, los otros servicios no podrían usarla sin crear
una dependencia circular (`common` → `status-service` → `common`) o duplicarla.
En `common` es accesible para todos sin ciclos.

## Bonus C — ¿Por qué la partition key es `fromAccount` y no `transactionId`?

**Con `transactionId` como key:**
Cada transferencia va a una partición aleatoria (hash del UUID).
Kafka garantiza orden FIFO dentro de la partición.
Como cada transferencia tiene su propio UUID, no hay colisión → cada una puede ir
a cualquier partición. Orden garantizado para esa transferencia específica, pero
no entre transferencias de la misma cuenta.

**Con `fromAccount` como key:**
TODAS las transferencias de `ACC-001` van a la misma partición.
Si `ACC-001` inicia Transfer-A y Transfer-B en ese orden:
Transfer-A va a partition-1 primero, luego Transfer-B va a partition-1.
`DebitConsumer` (que tiene 1 thread por partición) las procesa en ese orden.

Esto importa cuando hay verificaciones que dependen del orden:
- Límite diario de transferencias: no se puede verificar sin procesar en orden
- Bloqueo de cuenta: si la cuenta se bloquea en Transfer-A, Transfer-B no debería procesarse
- Auditoría: la secuencia de operaciones debe reflejar el orden de la solicitud

En el lab, con Optimistic Locking y saldos simples, el resultado numérico final
es el mismo independientemente del orden. Pero el patrón es correcto para producción.


