X_columns = [
    'flow_duration', 'Header_Length', 'Protocol Type', 'Duration',
    'Rate', 'Srate', 'Drate', 'fin_flag_number', 'syn_flag_number',
    'rst_flag_number', 'psh_flag_number', 'ack_flag_number',
    'ece_flag_number', 'cwr_flag_number', 'ack_count',
    'syn_count', 'fin_count', 'urg_count', 'rst_count',
    'HTTP', 'HTTPS', 'DNS', 'Telnet', 'SMTP', 'SSH', 'IRC', 'TCP',
    'UDP', 'DHCP', 'ARP', 'ICMP', 'IPv', 'LLC', 'Tot sum', 'Min',
    'Max', 'AVG', 'Std', 'Tot size', 'IAT', 'Number', 'Magnitue',
    'Radius', 'Covariance', 'Variance', 'Weight'
]
class_labels_Num = {
    'DDoS-UDP_Flood': 1,
    'DDoS-TCP_Flood': 2,
    'DDoS-ICMP_Flood': 3,
    'DDoS-ACK_Fragmentation': 4,
    'DDoS-UDP_Fragmentation': 5,
    'DDoS-HTTP_Flood': 6,
    'DDoS-SlowLoris': 7,
    'DDoS-ICMP_Fragmentation': 8,
    'DDoS-PSHACK_Flood': 9,
    'DDoS-SynonymousIP_Flood': 10,
    'DDoS-RSTFINFlood': 11,
    'DDoS-SYN_Flood': 12,

    'DoS-UDP_Flood': 13,
    'DoS-TCP_Flood': 14,
    'DoS-SYN_Flood': 15,
    'DoS-HTTP_Flood': 16,

    'DictionaryBruteForce': 17,


    'XSS': 18,
    'SqlInjection': 19,
    'BrowserHijacking': 20,
    'CommandInjection': 21,
    'Backdoor_Malware': 22,
    'Uploading_Attack': 23,

    'Recon-HostDiscovery': 24,
    'Recon-OSScan': 25,
    'Recon-PortScan': 26,
    'Recon-PingSweep': 27,
    'VulnerabilityScan': 28,

    'MITM-ArpSpoofing': 29,
    'DNS_Spoofing': 30,

    'Mirai-greeth_flood': 31,
    'Mirai-udpplain': 32,
    'Mirai-greip_flood': 33,

    'BenignTraffic': 0
}
