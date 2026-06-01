
# Fabric Map with real-time Boston Metro data (MBTA)
Want an easy-to-follow example of getting Real-time data into Fabric and visualizing it with Fabric Maps? 

This workbook will guide you through creating a Fabric Map with live MBTA bus locations, updating every 10 seconds.

* Quick and easy setup: A simple Notebook polls the MBTA feed and updates the Event stream - 
* When you run the notebook, the stream starts. 
* When you cancel the notebook run (or stop the Spark session), the stream stops. 
* Personally, I love the simplicity of having the on/off switch 
    * Running a spark job just for this is overkill for production

* Here's the step-by-step setup guide
    * Includes the Notebook code to poll the MBTA JSON feed every 10 seconds, 
    * Kusto query that creates the function you'll use as a data layer on your Map.

## [MBTA_Live_Map Playbook](MBTA_Live_Map_Playbook.md)