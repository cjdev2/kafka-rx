# 0.3.0

- Renamed `RxConnector` to `RxConsumer`
- Renamed `Message` to `Record`
- Extracted committer into trait
- Introduced `Committable[T]` for derived values
- `.produce` now returns kafka ProducerRecords

# 0.2.0

- Adding producing method `.saveToKafka`
- Adding k/v type parameters to message
- Adding kafka serializers to getMessageStream

# 0.1.0

- Initial release, reactive kafka consumer w/ offset tracking
