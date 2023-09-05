# IP="127.1"
# PORT=8081
python3 -m test.Producers.p1 > logs/log_p1&
python3 -m test.Producers.p2 > logs/log_p2& 
python3 -m test.Producers.p3 > logs/log_p3& 
python3 -m test.Producers.p4 > logs/log_p4& 
python3 -m test.Producers.p5 > logs/log_p5& 
python3 -m test.Consumers.c1 > logs/log_c1& 
python3 -m test.Consumers.c2 > logs/log_c2& 
python3 -m test.Consumers.c3 > logs/log_c3&

wait