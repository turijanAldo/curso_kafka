package com.lab.api.controller;

import com.lab.api.dto.TransferRequest;
import com.lab.api.dto.TransferResponse;
import com.lab.api.service.TransferService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * Controller REST del gateway de transferencias.
 *
 * Responsabilidades del controller (y SOLO estas):
 *   1. Recibir el request HTTP
 *   2. Delegar al Service
 *   3. Construir la respuesta HTTP con el código correcto
 *
 * ¿Por qué el controller NO tiene lógica de negocio?
 * El controller es la capa HTTP. Si mañana agregas GraphQL, gRPC o mensajería,
 * el Service no cambia. Solo agregas un nuevo "adaptador" que llama al mismo Service.
 * Si la lógica estuviera en el controller, tendrías que duplicarla.
 *
 * @RestController = @Controller + @ResponseBody
 *   @Controller → Spring registra esta clase como handler de requests HTTP
 *   @ResponseBody → cada método serializa su retorno directamente como JSON
 *                   en el cuerpo de la respuesta
 */
@RestController
@RequestMapping("/transfers")
@RequiredArgsConstructor
@Slf4j
public class TransferController {

    private final TransferService transferService;

    /**
     * POST /transfers
     * Inicia una nueva transferencia asíncrona.
     *
     * @Valid → activa Bean Validation sobre TransferRequest.
     * Si algún campo falla (@NotBlank, @Positive, etc.),
     * Spring lanza MethodArgumentNotValidException antes de
     * que el método se ejecute, y el @ExceptionHandler de abajo
     * la convierte en HTTP 400.
     *
     * HTTP 202 ACCEPTED (y no 201 CREATED) porque:
     *   201 Created → el recurso fue creado completamente y está listo
     *   202 Accepted → la solicitud fue aceptada pero el procesamiento
     *                  es asíncrono y aún no terminó
     * La transferencia entra en PROCESSING — el Saga puede tardar
     * varios segundos en completarse a través de 4 microservicios.
     */
    @PostMapping
    public ResponseEntity<TransferResponse> createTransfer(
            @Valid @RequestBody TransferRequest request) {

        log.info("POST /transfers | from={} to={} amount={}",
                request.getFromAccount(), request.getToAccount(), request.getAmount());

        // El txId lo genera TransferService internamente (UUID.randomUUID).
        // Lo ponemos en MDC DESPUÉS de la llamada para que el log de
        // "202 Accepted" y cualquier log posterior del request lleven el txId.
        // Los logs DENTRO de initiateTransfer() no tendrán txId en MDC —
        // eso es aceptable: la correlación cross-service empieza en los consumers.
        TransferResponse response = transferService.initiateTransfer(request);
        try {
            MDC.put("txId", response.getTransactionId());
            log.info("202 ACCEPTED | txId={} from={} to={} amount={}",
                    response.getTransactionId(),
                    request.getFromAccount(), request.getToAccount(), request.getAmount());
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
        } finally {
            MDC.remove("txId");
        }
    }

    /**
     * GET /transfers/{id}
     * Consulta el estado actual de una transferencia.
     *
     * @PathVariable extrae el {id} de la URL y lo pasa como parámetro.
     *
     * HTTP 200 OK con el DTO completo incluyendo el status actual.
     * El cliente puede hacer polling a este endpoint hasta que el status
     * sea COMPLETED, FAILED o ROLLED_BACK (estados terminales).
     */
    @GetMapping("/{id}")
    public ResponseEntity<TransferResponse> getStatus(@PathVariable String id) {

        log.info("GET /transfers/{}", id);

        TransferResponse response = transferService.getStatus(id);
        return ResponseEntity.ok(response);
    }

    /**
     * Handler para transferencias no encontradas.
     *
     * @ExceptionHandler(X.class) → Spring llama a este método cuando
     * cualquier método del controller lanza X (o sus subclases).
     *
     * Devuelve HTTP 404 NOT FOUND con un JSON de error descriptivo.
     * El mapa se serializa automáticamente como: {"error": "mensaje"}
     */
    @ExceptionHandler(TransferService.TransactionNotFoundException.class)
    public ResponseEntity<Map<String, String>> handleNotFound(
            TransferService.TransactionNotFoundException ex) {

        log.warn("Transferencia no encontrada: {}", ex.getMessage());
        return ResponseEntity
                .status(HttpStatus.NOT_FOUND)
                .body(Map.of("error", ex.getMessage()));
    }

    /**
     * Handler para errores de validación del @Valid.
     *
     * Cuando @Valid falla, Spring lanza MethodArgumentNotValidException.
     * Este handler la captura y construye un JSON de error legible
     * con todos los campos que fallaron y por qué.
     *
     * Sin este handler, Spring devolvería un JSON genérico con mucho
     * ruido interno que el cliente no necesita ver.
     */
    @ExceptionHandler(org.springframework.web.bind.MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, String>> handleValidationErrors(
            org.springframework.web.bind.MethodArgumentNotValidException ex) {

        // Recolectar todos los errores de validación en un mapa campo → mensaje
        Map<String, String> errors = new java.util.LinkedHashMap<>();
        ex.getBindingResult().getFieldErrors().forEach(fieldError ->
                errors.put(fieldError.getField(), fieldError.getDefaultMessage())
        );

        log.warn("Error de validación en request: {}", errors);
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errors);
    }
}
