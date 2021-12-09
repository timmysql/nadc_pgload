# Contributions
### FactContributions
- Note: Determine the filtering done on the PBI side and document.
- Source: dw.FactContributions
  - vLoad_FactContributions_New
    - vLoad_FactContributions
      - vLoad_Fact_B1AB
      - vLoad_Fact_B1C2
      - vLoad_Fact_B2B
      - vLoad_Fact_B2A
      - vLoad_Fact_B3_Raw
      - vLoad_Fact_B4A
      - vLoad_Fact_B5
      - vLoad_Fact_B72
      - vLoad_Fact_B73
      - vLoad_Fact_B9B
      - vLoad_Fact_CFLA8A
      - vLoad_Fact_CFLA8B
- PowerBI Filters
  - Keep = 1
  - each ([DuplicateDelete] <> 1) 
  - and ([Form] <> "B3" 
  - and [Form] <> "B9B" 
  - and [Form] <> "CFLA8A" 
  - and [Form] <> "CFLA8B") 
  - and ([UpdatedDeletedFromSource] = null))
### DimDate
- Source: dw.DimDate
  
### DimTransactionType
- dw.
- 
### DimSupportOppose

### DimRecipients
- Source vDimRecipients (dbo)
- vDimRecipients reads from dw.Entities

### DimCandidates
- Source: dw.DimCandidates 
  

### DimContributors
- Source: vDimContributors (dbo)
- vDimContributors reads from dw.Entities


### DimContributorType
- Source: dw.ContributorType