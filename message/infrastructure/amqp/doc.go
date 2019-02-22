// AMQP implementation of Watermill's Pub/Sub interface.
//
// Supported features:
// - Reconnect support
// - Fully customizable configuration
// - Qos settings
// - TLS support
// - Publish Transactions support (optional, can be enabled in config)
//
// Nomenclature
//
// Unfortunately, Watermill's nomenclature is not fully compatible with AMQP's nomenclature.
// Depending of the configuration, topic can be mapped to exchange name, routing key and queue name.
//
// IMPORTANT: Watermill's topic is not mapped directly to the AMQP's topic exchange type.
// It is used to generate exchange name, routing key and queue name, depending on the context.
// To check how topic is mapped, please check Exchange.GenerateName, Queue.GenerateName and Publish.GenerateRoutingKey.
//
// In case of any problem to find to what exchange name, routing key and queue name are set,
// just enable logging with debug level and check it in logs.
package amqp
