public class TrafficData
    {
        public string train_id { get; set; }
        public int trip_number { get; set; }
        public string direction { get; set; }
        public string previous_stop { get; set; }
        public string current_stop { get; set; }
        public string next_stop { get; set; }
        public string arrival_time { get; set; }
        public string departure_time { get; set; }
        public string duration_to_next { get; set; }
    }
