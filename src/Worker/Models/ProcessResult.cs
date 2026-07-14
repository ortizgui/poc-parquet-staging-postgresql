namespace PocWorker.Models;

public record ProcessResult(
    int Inserted,
    int Updated,
    int Invalid,
    int TotalRows,
    string SourceFile
);
