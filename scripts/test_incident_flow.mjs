import fetch from "node-fetch";

const TOMTOM_KEY = process.env.TOMTOM_KEY;

const incident = {
  id: 254168,
  route: "I-95",
  direction: "S",
  lat: 39.9636360609484,
  lon: -75.1389029458451,
  secondaryLat: 39.9408219644608,
  secondaryLon: -75.1435079382976
};

async function getFlow(lat, lon) {
  const url =
    `https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json` +
    `?key=${TOMTOM_KEY}` +
    `&point=${lat},${lon}` +
    `&unit=mph`;

  const r = await fetch(url);
  const j = await r.json();

  return j.flowSegmentData;
}

async function run() {
  const flow = await getFlow(incident.lat, incident.lon);

  console.log("Incident:", incident.id);
  console.log("Current speed:", flow.currentSpeed);
  console.log("Free flow speed:", flow.freeFlowSpeed);
  console.log("Road closure:", flow.roadClosure);
  console.log("Confidence:", flow.confidence);

  const ratio = flow.currentSpeed / flow.freeFlowSpeed;

  if (flow.currentSpeed <= 10 || ratio < 0.2) {
    console.log("Potential trapped queue conditions detected.");
  } else if (flow.currentSpeed <= 35 || ratio < 0.6) {
    console.log("Heavy backlog conditions detected.");
  } else {
    console.log("Traffic flowing normally.");
  }
}

run();
