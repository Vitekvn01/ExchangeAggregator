using ExchangeAggregator.Core.Models;

namespace ExchangeAggregator.Core.Interfaces;

/// <summary>
/// Парсер «сырого» JSON-сообщения от конкретной биржи в <see cref="RawTick"/>.
/// Каждая биржа имеет свою реализацию.
/// </summary>
public interface ITickParser
{
    /// <summary>Имя биржи, которую обслуживает парсер.</summary>
    string Exchange { get; }

    /// <summary>
    /// Пытается разобрать JSON-сообщение.
    /// Возвращает null, если сообщение не удалось разобрать (мусор/незнакомый формат).
    /// Выбрасывает исключение только при фатальных ошибках.
    /// </summary>
    RawTick? TryParse(string json);
}
