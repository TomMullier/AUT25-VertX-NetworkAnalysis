from flowmeter.flowmeter import Flowmeter
import pandas as pd

feature_gen = Flowmeter("../01_phase/pcap/reference.pcap")
df = feature_gen.build_feature_dataframe()


df.to_csv("./csv/flowmeter_flows.csv")