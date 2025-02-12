#used for calculating density
def teos_sigma0(T,S):
    import dask.array as da

    r1_T0 = 1./40.
    r1_S0 = 0.875/35.16504
    rdeltaS = 32.0
    EOS000 = 8.0189615746e+02
    EOS100 = 8.6672408165e+02
    EOS200 = -1.7864682637e+03
    EOS300 = 2.0375295546e+03
    EOS400 = -1.2849161071e+03
    EOS500 = 4.3227585684e+02
    EOS600 = -6.0579916612e+01
    EOS010 = 2.6010145068e+01
    EOS110 = -6.5281885265e+01
    EOS210 = 8.1770425108e+01
    EOS310 = -5.6888046321e+01
    EOS410 = 1.7681814114e+01
    EOS510 = -1.9193502195
    EOS020 = -3.7074170417e+01
    EOS120 = 6.1548258127e+01
    EOS220 = -6.0362551501e+01
    EOS320 = 2.9130021253e+01
    EOS420 = -5.4723692739
    EOS030 = 2.1661789529e+01
    EOS130 = -3.3449108469e+01
    EOS230 = 1.9717078466e+01
    EOS330 = -3.1742946532
    EOS040 = -8.3627885467
    EOS140 = 1.1311538584e+01
    EOS240 = -5.3563304045
    EOS050 = 5.4048723791e-01
    EOS150 = 4.8169980163e-01
    EOS060 = -1.9083568888e-01

    zt = T * r1_T0
    zs = da.sqrt( da.abs( S + rdeltaS ) * r1_S0 )

    zn0 = ((((((EOS060*zt   
        + EOS150*zs+EOS050)*zt    
        + (EOS240*zs+EOS140)*zs+EOS040)*zt    
        + ((EOS330*zs+EOS230)*zs+EOS130)*zs+EOS030)*zt    
        + (((EOS420*zs+EOS320)*zs+EOS220)*zs+EOS120)*zs+EOS020)*zt    
        + ((((EOS510*zs+EOS410)*zs+EOS310)*zs+EOS210)*zs+EOS110)*zs+EOS010)*zt   
        + (((((EOS600*zs+EOS500)*zs+EOS400)*zs+EOS300)*zs+EOS200)*zs+EOS100)*zs+EOS000)
    
    sigma0 = zn0
          
    return sigma0