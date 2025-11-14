import random
import time
import numpy as np
import logging
from benchmark.Producer.Producer import Producer
from typing import List

probs=[0.5,0.3,0.2] # view,click,purchase

class SteadyPoissonGenerator:
    def __init__(self,producer:Producer,throughput:int,user_ids:List[str],page_ids:List[str],ad_ids:List[str],ad_type_mapping:dict,event_type:List[str],topic:str):
        self.producer=producer
        self.throughput=throughput
        self.user_ids=user_ids
        self.page_ids=page_ids
        self.ad_ids=ad_ids
        self.ad_type_mapping=ad_type_mapping
        self.event_type=event_type
        self.ip_addresses = [f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}" 
                             for _ in range(100)]
        self.topic=topic

    def generate_events(self,windowid:int,simulated_time:int):
        t=simulated_time
        events_sent=0
        while True:
            t+=np.random.exponential(1e9/self.throughput)
            if t>simulated_time+1e9:
                break
            event={
                "user_id":random.choice(self.user_ids),
                "page_id":random.choice(self.page_ids),
                "ad_id":random.choice(self.ad_ids),
                "ad_type":self.ad_type_mapping[random.choice(self.ad_ids)],
                "ns_time":int(t),
                "ip_address":random.choice(self.ip_addresses),
                "window_id":windowid,
                "window_start_time":int(simulated_time),
                "event_type":random.choices(self.event_type,weights=probs)[0],
                "produce_time":int(time.time_ns()/1_000_000)
            }
            self.producer.send(self.topic,value=event)
            events_sent+=1
        return events_sent

class MMMPGenerator:
    def __init__(self,producer:Producer,throughput:int,lambda_H:int,d_H:int,d_L:int,user_ids:List[str],page_ids:List[str],ad_ids:List[str],ad_type_mapping:dict,event_type:List[str],topic:str):
        self.producer=producer
        self.throughput=throughput
        self.lambda_H=lambda_H
        self.d_H=d_H
        self.d_L=d_L
        self.topic=topic
        self.user_ids=user_ids
        self.page_ids=page_ids
        self.ad_ids=ad_ids
        self.ad_type_mapping=ad_type_mapping
        self.event_type=event_type
        self.ip_addresses = [f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}" 
                             for _ in range(100)]
    
    def generate_events(self,windowid,simulated_time):
        t = simulated_time
        lambda_H = self.lambda_H
        d_H = self.d_H
        d_L = self.d_L
        throughput = self.throughput

        if d_L == 0:
            lambda_L = 0
        else:
            lambda_L = (throughput * (d_H + d_L) - lambda_H * d_H) / d_L

        if lambda_L < 0:
            logging.warning("Calculated lambda_L is negative (%f). Adjusting to 0.", lambda_L)
            lambda_L = 0

        events_sent = 0
        state = 'HIGH'

        while True:
            state = 'HIGH' if random.random() > 0.5 else 'LOW'
            if state == 'HIGH':
                end_high = t + d_H * 1e9
                while t < end_high:
                    dt = -np.log(random.random()) / lambda_H * 1e9
                    t += dt
                    if t > simulated_time + 1e9:
                        return events_sent
                    event = {
                        "user_id": random.choice(self.user_ids),
                        "page_id": random.choice(self.page_ids),
                        "ad_id": random.choice(self.ad_ids),
                        "ad_type": self.ad_type_mapping[random.choice(self.ad_ids)],
                        "ns_time": int(t),
                        "ip_address": random.choice(self.ip_addresses),
                        "window_id": windowid,
                        "window_start_time": int(simulated_time),
                        "event_type": random.choices(self.event_type,weights=probs)[0],
                        "produce_time": int(time.time_ns()/1_000_000)
                    }
                    self.producer.send(self.topic, value=event)
                    events_sent += 1

            else:
                end_low = t + d_L * 1e9
                if lambda_L > 0:
                    while t < end_low:
                        dt = -np.log(random.random()) / lambda_L * 1e9
                        t += dt
                        if t > simulated_time + 1e9:
                            return events_sent
                        event = {
                            "user_id": random.choice(self.user_ids),
                            "page_id": random.choice(self.page_ids),
                            "ad_id": random.choice(self.ad_ids),
                            "ad_type": self.ad_type_mapping[random.choice(self.ad_ids)],
                            "ns_time": int(t),
                            "ip_address": random.choice(self.ip_addresses),
                            "window_id": windowid,
                            "window_start_time": int(simulated_time),
                            "event_type": random.choices(self.event_type,weights=probs)[0],
                            "produce_time": int(time.time_ns()/1_000_000)
                        }
                        self.producer.send(self.topic, value=event)
                        events_sent += 1
                else:
                    logging.warning("Skipping LOW state generation due to non-positive lambda_L.")
                    t = end_low  
