# OSRM-Distance-and-Duration
OSRM Distance and Route Calculation

Use this to run

<code>python distance.py --input coordinates.xlsx --output-prefix results --max-nodes 40 --concurrency 4 --clear-cache</code>

If you're getting high failures, reduce --max-nodes in multiples of 20 and --concurrency in multiples of 2.

I have had the optimal results with this combination. --max-nodes 40 --concurrency 4

The required columns are as below:
source latitude	| source longitude	| destination latitude	| destination longitude
![image](https://github.com/user-attachments/assets/b2dfebb7-4dd6-4341-b04f-1c91afae505e)

For more info, please go the below link:
https://project-osrm.org/docs/v5.7.0/api/?language=cURL#general-options
