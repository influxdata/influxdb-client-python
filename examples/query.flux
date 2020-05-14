import "date"

from(bucket: "my-bucket")
  |> range(start: -50d)
  |> filter(fn: (r) => r["_measurement"] == "weather" and r["_field"] == "temperature")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> map(fn: (r) => ({ r with weekDay: date.weekDay(t: r._time) }))
