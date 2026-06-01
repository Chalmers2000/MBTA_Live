
# MBTA_Live
Create a Fabric Map with live MBTA bus locations, updating every 10 seconds.

* I kept it simple with a Notebook that polls the feed and updates the Event stream - 
* When you run the notebook, the stream starts. 
* When you cancel the notebook (or stop the Spark session), the stream stops. 
* Personally, I love the simplicity of having the on/off switch, obviously you would productionalize this differently.

* Here's the step-by-step including the Notebook code to poll the MBTA JSON feed every 10 seconds, and the Kusto function you'll need to visualize on the Map

[MBTA_Live_Map Instructions](MBTA_Live_Map_Playbook.md)