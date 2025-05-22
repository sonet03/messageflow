namespace SharedLibrary;

public record OrderMessage(
    string OrderId,
    string UserId,
    string ProductId,
    int Quantity,
    decimal Price,
    DateTime Timestamp
);