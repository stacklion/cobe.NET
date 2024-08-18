# cobeNET

cobeNET is a C# implementation of the Cobe chatbot engine. It provides a framework for creating and training chatbots using Markov chain models.

## Project Structure

The project consists of several main components:

1. `Brain.cs`: Contains the core logic for the chatbot, including learning and reply generation.
2. `Graph.cs`: Implements the graph structure used to store and retrieve n-grams.
3. `Scoring.cs`: Defines various scoring mechanisms for evaluating potential replies.
4. `Tokenizer.cs`: Provides tokenization utilities for processing input text.
5. `SqliteHelper.cs`: Offers helper methods for SQLite database operations.
6. `StringHelper.cs`: Contains utility functions for string manipulation.
7. `TimeHelper.cs`: Provides time-related utility functions.

## Key Features

- Markov chain-based text generation
- Customizable tokenization (MegaHAL and Cobe tokenizers included)
- Stemming support for improved language understanding
- SQLite database for efficient storage and retrieval of n-grams
- Various scoring algorithms for reply selection

## Getting Started

To use cobeNET in your project:

1. Clone the repository
2. Add the project to your solution
3. Install the required NuGet packages (SQLite, MoreLinq, Serilog)
4. Initialize a new Brain instance:

```csharp
var brain = new Brain("path/to/database.sqlite");
```

5. Train the brain:

```csharp
brain.learn("Hello, how are you?");
```

6. Generate replies:

```csharp
string reply = brain.reply("How are you?");
Console.WriteLine(reply);
```

## Dependencies

- System.Data.SQLite
- MoreLinq
- Serilog
- Iveonik.Stemmers

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is open-source and available under the [MIT License](LICENSE).

## Acknowledgements

This project is based on the original Cobe chatbot engine. Special thanks to the creators and contributors of the original Cobe project.
