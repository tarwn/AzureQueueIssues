AzureQueueIssues
================

Code examples and tests for issues I am blogging about w/ Azure Queues

So far:

- PopReceiptMismatchReturnsWrongError.cs = Unit tests that show that the PopReceiptMismatch error does not work at all, 
  not even in an edge case from the MSDN forums. Equally doesn't work in local Emulator (Storage SDK 2.1) and Azure 
  (mix of Storage SDK 2.1 and handwritten REST, both against API 2012-02-12)
