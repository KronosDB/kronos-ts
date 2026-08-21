/**
 * A serialized representation of a value, carrying type information
 * for deserialization and upcasting.
 */
export type SerializedObject = {
  readonly type: string
  readonly revision: string
  readonly data: Uint8Array
}

/**
 * Core serializer interface — converts values between their in-memory
 * representation and serialized form (Uint8Array).
 *
 * The framework uses three serializer slots:
 * - **Default serializer** — fallback for all serialization
 * - **Message serializer** — for command and query message payloads
 * - **Event serializer** — for event payloads stored in the event store
 *
 * If a specialized serializer is not configured, the default is used.
 * This allows e.g. JSON for messages but Avro for events.
 */
export type Serializer = {
  /**
   * Serialize a value to its wire format.
   *
   * @param value The value to serialize
   * @param type The qualified type name (e.g., "university.CourseCreated")
   * @param revision The schema revision (e.g., "1.0")
   */
  serialize(value: unknown, type: string, revision?: string): SerializedObject

  /**
   * Deserialize a serialized object back to its in-memory representation.
   */
  deserialize<T>(data: SerializedObject): T

  /**
   * Returns true if this serializer can handle the given type.
   */
  canConvert(type: string): boolean
}

/**
 * A decorator that wraps a delegate serializer to add behavior
 * (upcasting, validation, encryption, compression, etc.)
 */
export type SerializerDecorator = (delegate: Serializer) => Serializer
