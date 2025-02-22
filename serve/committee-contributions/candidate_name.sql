SELECT cand_recipient FROM FEC.TRANSFORMED.fct_committee_contributions 
WHERE {{ Candidate }}
GROUP BY cand_recipient;