

echo "Starting table creation bronze ................"

#table creation bronze

sh /home/raptor/IdeaProjects/SparkLearning/temp/tableCreationBronze.sh

if [ $? == 0 ] then 
     echo "Starting table creation bronze ................"
     sh /home/raptor/IdeaProjects/SparkLearning/temp/tableAppendingBronze_1.sh
           if [ $? == 0 ] then
           sh /home/raptor/IdeaProjects/SparkLearning/temp/tableAppendingBronze_2.sh
	         if [ $? == 0 ] then
                 sh /home/raptor/IdeaProjects/SparkLearning/temp/tableAppendingBronze_3.sh
                 fi
           fi     
fi	



do it with sys.process._

