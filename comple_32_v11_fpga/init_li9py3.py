""" lijian xao @2023 for test zcu111 """
import casperfpga,time
import yaml
from datetime import datetime,timedelta

import binascii

fpgs='complex_iq_bw2048_out32_fixaxi_v12b_2025-10-04_2325.fpg'
print('test 47dr lijian 2025.03.5')
fpga=casperfpga.CasperFpga('192.168.162.53')
time.sleep(1)
fpga.upload_to_ram_and_program(fpgs)
time.sleep(1)
print(fpgs)
print("program fpgs done!")

fpga.adcs
rfdc_zcu111=fpga.adcs['rfdc']

#c1=rfdc_zcu111.show_clk_files()
#print(c1)


time.sleep(0.1)


rfdc_zcu111.init()
time.sleep(1)
print(rfdc_zcu111.status())

#rfdc_zcu111.run_mts(0xf)# first run 4 title mts 

time.sleep(0.1)

#rfdc_zcu111.progpll?
#fpga.write_int('start_data',0)
fpga.write_int('vdif_on',1)

time.sleep(0.01)
#fpga.write_int('pps_sim',0)




fpga.write_int('vdif_on',1)



#print  rfdc_zcu111.device_info
fpga.write_int('pkt_rst_0', 1)

time.sleep(0.1)
fpga.write_int('pkt_rst_0', 0)



config_file = 'config_1.yaml'
with open(config_file, 'r') as f:
      c = yaml.load(f, Loader=yaml.SafeLoader)


#----------------------------------------------------
print("fpga.gbes", fpga.gbes)
eth = fpga.gbes['onehundred_gbe_1']

ip0 = '10.17.16.60'
mac0 = c['arp']['f-engine'][ip0]

eth.configure_core(mac0, ip0, 60000)

for k, v, in c['arp'].items():
        print("eth0_Configuring arp values for {:s}".format(k))
        for ip, mac in v.items():
            print(ip, hex(mac))
            time.sleep(0.01)
            eth.set_single_arp_entry(ip, mac)
           
            
        print()
port_reform = lambda port_num: binascii.unhexlify('{:08x}'.format(port_num))

eth0_ip_ram = 'tx_dest_ip_ram'
eth0_port_ram = 'tx_dest_port_ram'

fpga.write(eth0_ip_ram, (bytes([10, 17, 16, 11])), offset=0x0)  # 128+-64

time.sleep(0.1)
fpga.write(eth0_ip_ram, (bytes([10, 17, 16, 12])), offset=0x4)

time.sleep(0.1)
#fpga.write(eth0_ip_ram, str(bytearray([10, 17, 16, 12])), offset=0x8)  # 256+-64
#fpga.write(eth0_ip_ram, str(bytearray([10, 17, 16, 12])), offset=0xc)  # 256+-64

fpga.write(eth0_port_ram, port_reform(17200), offset=0x0)
f
time.sleep(0.1)
fpga.write(eth0_port_ram, port_reform(17201), offset=0x4)


#fpga.write(eth0_port_ram, port_reform(17202), offset=0x8)
time.sleep(0.1)
#fpga.write(eth0_port_ram, port_reform(17204), offset=0xc)
time.sleep(0.1)




while(abs(datetime.utcnow().microsecond-5e5)>1e5):
    time.sleep(0.1)
fpga.write_int('onepps_ctrl',1<<31)
fpga.write_int('onepps_ctrl',0)

#######################################
# set headers
#######################################
# calculate reference epoch
utcnow = datetime.utcnow()
ref_start = datetime(2000,1,1,0,0,0)

nyrs = utcnow.year - ref_start.year 
ref_ep_num = 2*nyrs+1*(utcnow.month>6)

ref_ep_date = datetime(utcnow.year,6*(utcnow.month>6)+1,1,0,0,0) # date of start of epoch July 1 2014

##############
#   W0
##############
fpga.write_int('hdr0_rest',1)

#fpga.write_int('pack1_Subsystem_w0_rest',1)

# wait until middle of second for calculation
while(abs(datetime.utcnow().microsecond-5e5)>1e5):
    time.sleep(0.001)
   
# rapidly calculate current time and reset Subsystem (~10 ms) 
delta       = datetime.utcnow()-ref_ep_date
sec_ref_ep  = delta.seconds + 24*3600*delta.days

fpga.write_int('hdr0_rest',0)



fpga.write_int('hdr0_sec_ref_ep',sec_ref_ep)


print("base data start time is ref_ep_num@sec_ref_ep  {0:d}@{1:d}+{2:06d}".format(
        ref_ep_num,sec_ref_ep,0))
frame_dt = datetime(year = 2000+(ref_ep_num//2), month =1+ (ref_ep_num&1)*6, day = 1) + \
            timedelta(seconds = sec_ref_ep)
print(frame_dt.isoformat(' '))
#############
#   W1
#############
#print "reference epoch number: %d" %ref_ep_num
fpga.write_int('hdr0_ref_ep_num',ref_ep_num)


#############
#   W2
#############

# nothing to do
time.sleep(0.1)

############
#   W3 
############
thread_id_0=0
fpga.write_int('hdr0_w3_thread_id', 0)
#fpga.write_int('hdr_w3_thread_id2', 1)


station_id_0 = 'Ur'

# convert chars to 16 bit int
st0 = ord(station_id_0[0])*2**8 + ord(station_id_0[1])
#st1 = ord(station_id_1[0])*2**8 + ord(station_id_1[1])



fpga.write_int('hdr0_w3_station_id', st0)
#fpga.write_int('pack_hdr_w3_station_id', st1)



############
#   W4
############

eud_vers = 0x02

w4_0 = 0x00#eud_vers*2**24 + rec_sb0*4 + bdc_sb0*2 + pol_block0


fpga.write_int('hdr0_w4',w4_0)
#fpga.write_int('pack_hdr_w4',w4_1)

############
#   W5
############

# the offset in FPGA clocks between the R2DBE internal pps
# and the incoming GPS pps

############
#   W6
############

#  PSN low word, written by FPGA to VDIF header

###########
#   W7
############

# PSN high word, written by FPGA to VDIF header
#while(abs(datetime.utcnow().microsecond-5e5)>1e5):
#    time.sleep(0.1)

time.sleep(1)

fpga.write_int('start_data',1) # to sync start frame with internal pps, which started by external pps. (@fxzjshm, 2024.01.17; from lijian @ 2024.01.16)
print(fpga.estimate_fpga_clock())

print('done')
