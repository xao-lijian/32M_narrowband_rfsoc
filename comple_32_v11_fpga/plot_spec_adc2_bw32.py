#!/usr/bin/env python
'''
This script demonstrates programming an FPGA, configuring a wideband spectrometer and plotting the received data using the Python KATCP library along with the katcp_wrapper distributed in the corr package. Designed for use with TUT3 at the 2009 CASPER workshop.\n

You need to have KATCP and CORR installed. Get them from http://pypi.python.org/pypi/katcp and http://casper.berkeley.edu/svn/trunk/projects/packetized_correlator/corr-0.4.0/
./nspecv2.py 210.73.36.102 -b nspec1515.bof -l 1000
\nAuthor: Jason Manley, November 2009.
'''

#TODO: add support for ADC histogram plotting.
#TODO: add support for determining ADC input level 

import casperfpga,time,numpy,struct,sys,logging,pylab,matplotlib,math
import numpy as np
zcu111 = '192.168.162.53'
katcp_port=7147
bits=12

f_lo=16
bw=2*f_lo

cc1=0

def decode2(v):
  
  mask = 1 << (bits-1)
  return -(v & mask) | (v & (mask-1))

def decode1(d):
    d=d>>4
    
    if d >2048:
        d = 4096-d
        d = -d
    else:
        d = d #-8192
    return d



def plot_adc():
        matplotlib.pyplot.clf()
	fpga.write_int('adc_trig',1)
        time.sleep(0.01)
        fpga.write_int('adc_trig',0)

        adc0=np.array(struct.unpack('>8192h',fpga.read('b2_bram',4096*4,0)))
        #adc0y=np.array(struct.unpack('>32768h',fpga.read('snap_8ch_b2_bram',4096*4,0)))
        realX = adc0[0::2]
        imagX = adc0[1::2]
        #realX = adc0x[0:]
        #imagX = adc0y[0:]
        print('get1\n')
        #cc=cc+1
        global cc1
        cc1=cc1+1
        print(cc1)
        fft=2048
        matplotlib.pyplot.clf() 
        f = np.linspace(f_lo - bw/2., f_lo + bw/2., fft)

        X = np.zeros(4096, dtype=np.complex64)
        X.real = realX.astype(np.float)
        X.imag = imagX.astype(np.float)
       

        paa=[]
        #fft_a=np.fft.fftshift(np.fft.fft(a1, fft))
        fft_a=np.fft.fftshift(np.fft.fft(X, fft))
        #fft_a=(np.fft.fft(X, fft))
        log_a=10 * np.log10(np.abs(fft_a)+1)-45-15-20
        for k in range(fft):
           paa.append(log_a[k])
   
	#matplotlib.pyplot.subplot(111)
        #matplotlib.pyplot.ylim(15,50)
    	pylab.title('baseband32')	
	
        matplotlib.pyplot.plot(f,paa,'b')


        fig.canvas.draw()
    
        fig.canvas.manager.window.after(100, plot_adc)

    


#START OF MAIN:

if __name__ == '__main__':



    fpga = casperfpga.CasperFpga(zcu111)
    time.sleep(1)

    if fpga.is_connected():
        print 'ok\n'
    else:
        print 'ERROR connecting to server %s on port %i.\n'%(roach,katcp_port)
        exit_fail()

    print '------------------------'

    fig = matplotlib.pyplot.figure()
    ax = fig.add_subplot(1,1,1)

    # start the process
    fig.canvas.manager.window.after(500, plot_adc)
    matplotlib.pyplot.show()
    print 'Plot started.'









