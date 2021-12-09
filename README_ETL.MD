# ETL Workflow
## dw.spLoad_ALL_MASTER
1. dw.spLoad_Dim_MASTER
2. dw.spLoad_Fact_MASTER
3. twt.spLoad_TweetBot_MASTER
4. dw.spLoad_PostExtra_MASTER


## dw.spLoad_Dim_MASTER
2. dw.spLoad_DimCandidates
3. dw.spLoad_DimCommittees
4. dw.spLoad_DimContributors
5. dw.spLoad_DimLobbyists
6. dw.spLoad_DimPrincipals
7. dw.spLoad_DimTransactionType
8. dw.spLoad_DimEntities
9. ent.spLoadEntitiesWithNoIds_Raw
10. 
    `UPDATE noid
    SET [RealEntityKey] = EntityKey 
        ,[RealEntityID] = EntityID
    FROM [ent].[EntitiesWithNoIds_Raw] noid
    JOIN [dw].[DimEntities] de
    ON noid.EntityName = de.EntityName` 
11. dw.spLoad_DimEntities_Raw
12. dw.spLoad_DimEntities_PostRaw
13. dw.spLoad_DimMulti_PopulateDimEntityKeys
14. dw.spLoad_DimEntities_Cleanup


  
## dw.spLoad_Fact_MASTER
1. dw.spLoad_FactContributions
2. dw.spLoad_DimMulti_UpdateFlags
3. dw.spLoad_FactContributions_UpdateParentContributors
4. dw.spLoad_FactContributions_SetMostRecentDates 
5. dw.spLoad_FactContributions_Deduplicate
6. dw.spLoad_FactExpenditures
7. dbo.spLoadFormA1

## twt.spLoad_TweetBot_MASTER
1. twt.spLoad_TweetBotEntityWatchers
2. twt.spLoadTweetBotDonationWatche
3. twt.spLoadPeteBotDonationWatcher
4. twt.spLoadTweetBotJeanBothertDonationWatcher
5. twt.spLoadTweetBotCommitteeWatcher]

## dbo.spLoad_PostExtra_MASTER
1. dw.spLoad_B1_AdditionalBallotQuestionFilingType
2. dw.spLoad_B1_NatureOfFiling
3. dw.spLoad_B1_TypeOfFiling
4. dw.spLoadFact_B1_CampaignBallotQuestionSummaries

