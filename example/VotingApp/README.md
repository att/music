
The voting app for MUSIC  illustrates the features of MUSIC as a
multi-site state management
service. It is a program that
maintains state in MUSIC in the form of a vote-count table that has two columns, the candidateName
and his voteCount. To the external client that is using the voting app to update votes, the
votingApp provides a simple function to update the
votecount of the candidate in an exclusive manner. This is possible because of the locking service
in MUSIC. Since each candidate is a key in MUSIC, the votingapp simply acquires the geo-distributed
lock and only then upates the count. This guarantees correctness even when clients access different
end-points across data centers. Further since state is replicated correctly across data-centers,
even when one of the data centers fail, functioning can continue simply by using the MUSIC end point
in the other data center. 

The main function in the VotingApp.java is emulating clients from different data centers by randomly
chosing MUSIC end points. By updating vote counts
randomly using different MUSIC end points and still receiving the correct total count when doing a
read, the main function indicates the multi-site capability of MUSIC. 


To run the application, make sure you have onboarded the application using music's admin api.
A curl call using the default values would be:
curl -X POST \
  http://localhost:8080/MUSIC/rest/v2/admin/onboardAppWithMusic \
  -H 'Content-Type: application/json' \
  -d '{
"appname"  : "votingapp",
"userId"   : "abc123d",
"password" : "password",
"isAAF"    : false
}'
