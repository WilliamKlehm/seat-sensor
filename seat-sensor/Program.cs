using Confluent.Kafka;
using Newtonsoft.Json;

Console.WriteLine("Starting seat-sensor Programm...");
Console.WriteLine("Enter Start Date for Data generation (yyyy-MM-dd):");
string startDateString = Console.ReadLine();
Console.WriteLine("Enter End Date for Data generation (yyyy-MM-dd):");
string endDateString = Console.ReadLine();
Console.WriteLine("Search for traffic-schedule data...");

DateTime startDate = DateTime.Parse(startDateString);
DateTime endDate = DateTime.Parse(endDateString);

int totalDays = ( endDate - startDate).Days;

string[] files = Directory.GetFiles("traffic-schedule", "*.json");
if (files.Length == 0)
{
    Console.WriteLine("No traffic-schedule data found.");
    return;
}
else
{
    var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
    using (var producer = new ProducerBuilder<Null, string>(config).Build())
    {
        //read traffic-schedule data for each day
        for (int i = 0; i < totalDays; i++)
        {       
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
                    int minute = Convert.ToInt32(data.departure_time.Substring(3, 2));

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
                    for (int j = 1; j <= train_parts; j++)
                    {
                        DateTime currentdate = startDate.AddDays(i);
                        SeatSensorData seatSensorData = new SeatSensorData();
                        seatSensorData.train_id = data.train_id;
                        seatSensorData.timestamp = new DateTime(currentdate.Year, currentdate.Month, currentdate.Day, hour, minute, 0).ToString("yyyy-MM-dd HH:mm:ss");
                        seatSensorData.line_id = data.train_id.Substring(1, 1);
                        seatSensorData.train_part = j;
                        seatSensorData.total_seats = 100;
                    
                        int percentace = new Random().Next(percentageStart, percentageEnd);
                        seatSensorData.occupied_seats = seatSensorData.total_seats * percentace / 100;

                        string seatSensorDataJson = JsonConvert.SerializeObject(seatSensorData);

                        //produce seat-sensor data
                        try
                        {
                            var deliveryReport = await producer.ProduceAsync("seat-sensor", new Message<Null, string> { Value = seatSensorDataJson });
                            Console.WriteLine($"Produced to partition {deliveryReport.Partition} at offset {deliveryReport.Offset}");
                        }
                        catch (ProduceException<Null, string> ex)
                        {
                            Console.WriteLine($"Failed to produce message: {ex.Error.Reason}");
                        }

                        
                    }
                }
            }
        }
    }
}