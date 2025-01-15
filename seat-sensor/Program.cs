using Confluent.Kafka;
using Newtonsoft.Json;

Console.WriteLine("Starting seat-sensor Programm...");
Console.WriteLine("Search for traffic-schedule data...");

string[] files = Directory.GetFiles("traffic-schedule", "*.json");
if (files.Length == 0)
{
    Console.WriteLine("No traffic-schedule data found.");
    return;
}else{
    foreach (string file in files)
    {
        Console.WriteLine($"Reading file {file}...");
        string json = File.ReadAllText(file);
        List<TrafficData> trafficData = JsonConvert.DeserializeObject<List<TrafficData>>(json);
        foreach (TrafficData data in trafficData)
        {
            int train_parts;

            //between 22-6 night only 1 part 
            //6-12 morning 2 part 
            //12-18 afternoon 3 part 
            //18-22 evening 2 part
            int hour = Convert.ToInt32(data.departure_time.Substring(0, 2));
            if (hour >= 22 || hour < 6)
            {
                train_parts = 1;
            }
            else if (hour >= 12 && hour < 18)
            {
                train_parts = 3;
            }
            else
            {
                train_parts = 2;
            }
            int percentageStart;
            int percentageEnd;
            if (hour >= 6 && hour < 9)
            {
                percentageStart = 0;
                percentageEnd = 30;
            }
            else if (hour >= 9 && hour < 12)
            {
                percentageStart = 10;
                percentageEnd = 40;
            }
            else if (hour >= 12 && hour < 15)
            {
                percentageStart = 25;
                percentageEnd = 50;
            }
            else if (hour >= 15 && hour < 18)
            {
                percentageStart = 30;
                percentageEnd = 70;
            }
            else if (hour >= 18 && hour < 22)
            {
                percentageStart = 35;
                percentageEnd = 50;
            }
            else
            {
                percentageStart = 0;
                percentageEnd = 15;
            }
            for (int i = 1; i <= train_parts; i++)
            {
                SeatSensorData seatSensorData = new SeatSensorData();
                seatSensorData.train_id = data.train_id;
                seatSensorData.timestamp = data.departure_time;
                seatSensorData.line_id = data.train_id.Substring(1, 1);
                seatSensorData.train_part = i;
                seatSensorData.total_seats = 100;
            
                int percentace = new Random().Next(percentageStart, percentageEnd);
                seatSensorData.occupied_seats = seatSensorData.total_seats * percentace / 100;

                string seatSensorDataJson = JsonConvert.SerializeObject(seatSensorData);

                //conntect to local kafka broker
                var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
                using (var producer = new ProducerBuilder<Null, string>(config).Build())
                {
                    producer.Produce("seat-sensor", new Message<Null, string> { Value = seatSensorDataJson });
                    Console.WriteLine($"Produced seat-sensor data: {seatSensorDataJson}");
                }
            }
        }
    }
}