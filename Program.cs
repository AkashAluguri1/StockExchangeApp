using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

public class Order
{
    public string Type { get; }  // Buy or Sell
    public string Ticker { get; }  // Stock Symbol
    public int Quantity { get; set; }  // Number of Stocks
    public double Price { get; }  // Price per Stock

    public Order(string type, string ticker, int quantity, double price)
    {
        Type = type;
        Ticker = ticker;
        Quantity = quantity;
        Price = price;
    }
}

public class StockExchange
{
    private readonly ConcurrentQueue<Order> orderQueue = new();  // Lock-free queue for incoming orders
    private readonly PriorityQueue<Order, double> buyOrders = new();  // Max-Heap (Negated Price)
    private readonly PriorityQueue<Order, double> sellOrders = new(); // Min-Heap
    private readonly HashSet<string> tickers = new();  // Ensures a max of 1024 tickers
    private static int tickerCount = 0;
    private static readonly object tickerLock = new();

    // Add Order to queue (thread-safe)
    public void AddOrder(string type, string ticker, int quantity, double price)
    {
        lock (tickerLock)
        {
            if (!tickers.Contains(ticker))
            {
                if (tickerCount >= 1024)
                {
                    Console.WriteLine($"Ticker limit reached! Cannot add {ticker}.");
                    return;
                }
                tickers.Add(ticker);
                tickerCount++;
            }
        }

        orderQueue.Enqueue(new Order(type, ticker, quantity, price));
    }

    // Process queue and add to buy/sell heaps
    public void ProcessOrders()
    {
        while (!orderQueue.IsEmpty)
        {
            if (orderQueue.TryDequeue(out Order? order))
            {
                if (order.Type == "Buy")
                    buyOrders.Enqueue(order, -order.Price);  // Max-Heap for Buy Orders
                else
                    sellOrders.Enqueue(order, order.Price);  // Min-Heap for Sell Orders
            }
        }
    }

    // Match Buy and Sell Orders
    public void MatchOrders()
    {
        while (buyOrders.Count > 0 && sellOrders.Count > 0)
        {
            var buy = buyOrders.Peek();
            var sell = sellOrders.Peek();

            if (buy.Ticker == sell.Ticker && buy.Price >= sell.Price)
            {
                int matchedQuantity = Math.Min(buy.Quantity, sell.Quantity);
                Console.WriteLine($"Matched: {matchedQuantity} {buy.Ticker} at ${sell.Price}");

                // Adjust remaining quantities
                buy.Quantity -= matchedQuantity;
                sell.Quantity -= matchedQuantity;

                // Remove fully executed orders
                if (buy.Quantity == 0) buyOrders.Dequeue();
                if (sell.Quantity == 0) sellOrders.Dequeue();
            }
            else
            {
                break;  // No possible match
            }
        }
    }
}

// ------ TESTING ------
class Program
{
    static void Main()
    {
        StockExchange exchange = new();

        // Simulate concurrent order placement
        Task t1 = Task.Run(() => exchange.AddOrder("Buy", "AAPL", 10, 150));
        Task t2 = Task.Run(() => exchange.AddOrder("Sell", "AAPL", 5, 140));
        Task t3 = Task.Run(() => exchange.AddOrder("Sell", "AAPL", 7, 145));
        Task t4 = Task.Run(() => exchange.AddOrder("Buy", "AAPL", 8, 155));

        Task.WaitAll(t1, t2, t3, t4);  // Ensure all tasks complete

        // Process and match orders
        exchange.ProcessOrders();
        exchange.MatchOrders();
    }
}