# Contributions
## FactContributions


- ### DimDate
  - Relates: FactContribitions
  - DateKey: ContributionDateKey
  
- ### DimTransactionType
  - Relates: FactContribitions
  - TransactionType: TransactionType

- ### DimSupportOppose
  - Relates: SupportOppose
  - SupportOppose: SupportOppose

- ### DimRecipients
  - Relates: FactContributions
  - RecipientEntityKey: RecipientEntityKey

- ### DimCandidates
  - Relates: DimRecipients
  - CandidateEntityKey: SupportCandidateEntityKey
  

- ### DimContributors
  - Relates: FactContribtions
  - ContributorEntityKey: ContributorEntityKey


- ### DimContributorType
  - Relates: DimContributors
  - ContributorType: ContributorType