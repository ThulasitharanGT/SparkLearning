Run a producer from 2.4.4 spark kafka push data to producer script's.

readextractload sh. triggres the sparksubmit to read from kafka producer and push it to delta table. ----1

readdeltastream sh. triggres the sparksubmit to read from delta table which is being fed every five second by the previous stream producer job and  display's it in console as of now. more things can be done to save that data and crunch it.

statssh. triggres the sparksubmit to read from delta table which is being fed every five second by the previous stream producer job and  creates an agg of msg's per topic per date and stores in another location.
