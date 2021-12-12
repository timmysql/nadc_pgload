CREATE TABLE committee_type (
committee_type text
, committee_type_desc text
)


insert into committee_type
SELECT 'C' as committee_type,	'Candidate Committee' as committee_type_desc UNION
SELECT 'B',	'Ballot Question' UNION
SELECT 'P',	'Political Action Committee' UNION
SELECT 'T',	'Political Party Committee' UNION
SELECT 'I',	'Independent Reporting Committee' UNION
SELECT 'R',	'Independent Reporting Committee' UNION
SELECT 'S',	'Separate Segregated Political Fund Committee'