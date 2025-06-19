create or replace procedure RPT_insaebln202505001
(
    vnama varchar(512),
    vlokasi varchar(255),
    vwilayah varchar(255),
    vemployee varchar(255),
    vtipeperiode int,
    vtahun int,
    vperiode int,
    vposisi int,
    vtt int,
    vuser varchar(255),
    vreqkey varchar(255),
    vdate varchar(255),
    nomorsurat int,
    vdepolp int,
    vtxt int
) language PLvSQL as $$

declare
    periodhistory varchar(255);
    vtahunlalu int;
    datglawal date;
    datglakhir date;

    vtahunhistory int;
    vbulanhistory int;

    nosurat varchar(255);

    vbulan int;

    charea varchar(255);
    vketemployee varchar(255);
    vketposisi varchar(255);
    waktusaatini timestamp;

    vEntity varchar(255);

    stdInsCA int;
    stdInsNOC int;
    maxdate date;

    vPath varchar(255);
    mysetdir varchar(255);
    vnmfile2 varchar(255);
    vfile2 varchar(255);

    errCode VARCHAR(255);

begin
if 1=1 then
    vEntity := SELECT chvalue FROM lp_mreportfilter WHERE chkey = 'db';

    nosurat := '001-CEO-PPI-V-25';

    MySetDir := '/dwh/'||vEntity||'/report/rutin/insentif/insaebln202505001/'||vnama||'.csv';

    vPath := SUBSTR(MySetDir,1,INSTR(MySetDir, '/', -1)-1);
    vnmfile2 := vPath||'/'||vuser||'-Loadins-1-'||vreqkey||'-'||vdate||'.txt';
    vFile2 := REPLACE(SUBSTR(vnmfile2,INSTR(vnmfile2, '/', -1)+1),'.txt','');

    waktusaatini := select now();

    vbulan := vperiode;

    vketemployee :=
    case
        when vtipeperiode in (2) then 'AE'
        when vtipeperiode in (3) then 'AAM'
        when vtipeperiode in (4) then 'RBM'
    end;

    vketposisi :=
    case
        when vposisi in (0) then 'Last'
        when vposisi in (1) then 'Current'
    end;

    stdInsCA := 120 * 0.8;
    stdInsNOC := 10;

    maxdate := select max(datglcutoff) from lp_tpiutang where year(datglcutoff) = vtahun and month(datglcutoff) = vbulan;

    datglawal := select min(datgl) from lp_mperiod where intahun = vtahun and inbulan = vbulan;
    datglakhir := select max(datgl) from lp_mperiod where intahun = vtahun and inbulan = vbulan;

    vtahunlalu := vtahun - 1;

    if vposisi in (0) then
        periodhistory := select max(intahun||right('00'||inbulan,2)) from lp_mdepo_history;
        vtahunhistory := left(periodhistory,4)::int;
        vbulanhistory := right(periodhistory,2)::int;
    end if;

    perform create local temporary table if not exists wilayah (wil int) on commit preserve rows;

    perform insert into wilayah
    select split_part(vwilayah, '-', period_key)::int from dual
    cross join (select period_key from lp_mperiod where period_key <= regexp_count(vwilayah, '-')+1) a
    ;

    perform create local temporary table if not exists produkPPI
    (
        product_key int,chkdbarang varchar(255),chkp varchar(255),chklasifikasi varchar(255),chkpprodukmodel varchar(255),
        chpt varchar(255),chflagitemcustom varchar(255),chflagaktif varchar(255),itemfestive varchar(255)
    ) on commit preserve rows;

    if vposisi in (1) then
        perform insert into produkPPI
        select product.product_key,product.chkdbarang,chkp,chklasifikasi,chkpprodukmodel,
        chpt,chflagitemcustom,chflagaktif,chflagitemfestive
        from lp_mproduct product
        left join (
            select chkdbarang,chkp,chklasifikasi,chkpprodukmodel,chpt,chflagitemfestive
            chflagitemcustom,chflagaktif,chflagitemfestive
            from PPI_mItem
        ) b on product.chkdbarang = b.chkdbarang
        ;
    end if;

    if vposisi in (0) then
        perform insert into produkPPI
        select product.product_key,product.chkdbarang,chkp,chklasifikasi,chkpprodukmodel,
        chpt,chflagitemcustom,chflagaktif,chflagitemfestive
        from lp_mproduct_history product
        left join (
            select chkdbarang,chkp,chklasifikasi,chkpprodukmodel,chpt,chflagitemfestive
            chflagitemcustom,chflagaktif,chflagitemfestive
            from PPI_mItem
        ) b on product.chkdbarang = b.chkdbarang
        and intahun = vtahunhistory and inbulan = vbulanhistory
        ;
    end if;

    perform create local temporary table if not exists customer
    (
        customer_key int,inkdwilayah int,chketwilayah varchar(255),inkdcabang int,chketcabang varchar(255),
        inkddepo int,chketdepo varchar(255),chkdsite varchar(255),chkdcustomer varchar(255),chnamacustomer varchar(255),
        chkdemployee varchar(255),chkdda varchar(255),chNamaEmp varchar(255),datglmulaitransaksi date
    ) on commit preserve rows;

    if vposisi in (1) then
        if vdepolp in (0,1) then
            perform insert into customer
            select customer_key,cust.inkdwilayah,cust.chketwilayah,cust.inkdcabang,cust.chketcabang,cust.inkddepo,cust.chketdepo,cust.chkdsite,
            chkdcustomer,isnull(chnamacustomer,'N/A') namaCust,chkdemployee,cust.chkdda,chNamaEmp,datglmulaitransaksi
            from lp_mcustomer cust
            inner join (
                select distinct chkdda,'9'||substring(chkdemp,2) chkdemployee,max(isnull(chNamaEmp,'N/A')) chNamaEmp
                from del_PPI_mInsDALoad
                where chJabatan in (vketemployee) and chDivisi in ('B2B')
                group by chkdda,chkdemp
            ) emp on cust.chkdda = emp.chkdda
            where cust.inkdwilayah in (select wil from wilayah) and datglmulaitransaksi is not null
            ;
        end if;

        if vdepolp in (2) then
            perform insert into customer
            select customer_key,cust.inkdwilayah,cust.chketwilayah,cust.inkdcabang,cust.chketcabang,cust.inkddepo,cust.chketdepo,cust.chkdsite,
            chkdcustomer,isnull(chnamacustomer,'N/A') namaCust,chkdemployee,cust.chkdda,chNamaEmp,datglmulaitransaksi
            from lp_mcustomer_aarta cust
            inner join (
                select distinct chkdda,'9'||substring(chkdemp,2) chkdemployee,max(isnull(chNamaEmp,'N/A')) chNamaEmp
                from del_PPI_mInsDALoad
                where chJabatan in (vketemployee) and chDivisi in ('B2B')
                group by chkdda,chkdemp
            ) emp on cust.chkdda = emp.chkdda
            where cust.inkdwilayah in (select wil from wilayah) and datglmulaitransaksi is not null
            ;
        end if;
    end if;

    if vposisi in (0) then
        if vdepolp in (0,1) then
            perform insert into customer
            select customer_key,cust.inkdwilayah,cust.chketwilayah,cust.inkdcabang,cust.chketcabang,cust.inkddepo,cust.chketdepo,cust.chkdsite,
            chkdcustomer,isnull(chnamacustomer,'N/A') namaCust,chkdemployee,cust.chkdda,datglmulaitransaksi
            from lp_mcustomer_history cust
            inner join (
                select distinct chkdda,'9'||substring(chkdemp,2) chkdemployee,max(isnull(chNamaEmp,'N/A')) chNamaEmp
                from del_PPI_mInsDALoad
                where chJabatan in (vketemployee) and chDivisi in ('B2B')
                group by chkdda,chkdemp
            ) emp on cust.chkdda = emp.chkdda
            where cust.inkdwilayah in (select wil from wilayah) and datglmulaitransaksi is not null
            and intahun = vtahunhistory and inbulan = vbulanhistory
            ;
        end if;

        if vdepolp in (2) then
            perform insert into customer
            select customer_key,cust.inkdwilayah,cust.chketwilayah,cust.inkdcabang,cust.chketcabang,cust.inkddepo,cust.chketdepo,cust.chkdsite,
            chkdcustomer,isnull(chnamacustomer,'N/A') namaCust,chkdemployee,cust.chkdda,datglmulaitransaksi
            from lp_mcustomer_aarta_history cust
            inner join (
                select distinct chkdda,'9'||substring(chkdemp,2) chkdemployee,max(isnull(chNamaEmp,'N/A')) chNamaEmp
                from del_PPI_mInsDALoad
                where chJabatan in (vketemployee) and chDivisi in ('B2B')
                group by chkdda,chkdemp
            ) emp on cust.chkdda = emp.chkdda
            where cust.inkdwilayah in (select wil from wilayah) and datglmulaitransaksi is not null
            and intahun = vtahunhistory and inbulan = vbulanhistory
            ;
        end if;
    end if;

    /*
     ** Tipe Omset Classification (inTipeOms) **
     ** 0: target KP
     ** 1: target TOTAL (GLOBAL)
     ** 2: omset KP
     ** continue here...
     */

    perform create local temporary table if not exists prelistlt
    (
        inTipeOms int,intahun int,inbulan int,inkdwilayah int,inkdcabang int,inkddepo int,
        chkdemployee varchar(255),chNamaEmp varchar(255),chkdda varchar(255),chkdsite varchar(255),
        chkdcustomer varchar(255),chNamaCustomer varchar(255),chTipeKp varchar(255),chkp varchar(255),inTahunMulaiTrx int,inBulanMulaiTrx int,
        deQtyNetto dec(25,6),deRpNetto dec(25,6),loCustomerBaru boolean
    ) on commit preserve rows;

    perform insert into prelistlt
    select 0 inTipeOms,null intahun,null inbulan,null inkdwilayah,null inkdcabang,null inkddepo,
    null chkdemployee,null chNamaEmp,null chkdda,null chkdsite,
    null chkdcustomer,null chNamaCustomer,chproduk chTipeKp,chketproduk chkp,null inTahunMulaiTrx,null inBulanMulaiTrx,
    deTarget deQtyTarget,null deRpNetto,null loCustomerBaru
    from (
        select chketproduk,deTarget,chproduk
        from del_PPI_mInsTargetLoad
        where chJabatan in (vketemployee) and chproduk in ('KP')
        and intahun = vtahun and inbulan = vbulan and inkdwil in (select wil from wilayah)
    ) a
    ;

    perform insert into prelistlt
    select 1 inTipeOms,null intahun,null inbulan,null inkdwilayah,null inkdcabang,null inkddepo,
    null chkdemployee,null chNamaEmp,null chkdda,null chkdsite,
    null chkdcustomer,null chNamaCustomer,chproduk chTipeKp,chketproduk chkp,null inTahunMulaiTrx,null inBulanMulaiTrx,
    deTarget deQtyTarget,null deRpNetto,null loCustomerBaru
    from (
        select deTarget,chproduk,chketproduk
        from del_PPI_mInsTargetLoad
        where chJabatan in (vketemployee) and chproduk in ('T')
        and intahun = vtahun and inbulan = vbulan and inkdwil in (select wil from wilayah)
    ) a
    ;

    perform insert into prelistlt
    select 2 inTipeOms,intahun,inbulan,inkdwilayah,inkdcabang,inkddepo,chkdemployee,chNamaEmp,chkdda,chkdsite,
    chkdcustomer,chNamaCustomer,null chtipekp,chkp,inTahunMulaiTrx,inBulanMulaiTrx,
    sum(deqtynetto) deqtynetto,sum(derpnetto) derpnetto,
    case when inTahunMulaiTrx = inTahun and inBulanMulaiTrx = inBulan then 1 else 0 end loCustomerBaru
    from (
        select product_key,customer_key,deqtynetto,derpnetto,intahun,inbulan
        from dm_tjual_mon
        where intahun = vtahun and inbulan = vbulan
    ) a
    inner join (
        select customer_key,inkdwilayah,inkdcabang,inkddepo,chkdemployee,chnamaemp,chkdsite,chkdcustomer,chNamaCustomer,
        chkdda,year(datglmulaitransaksi::date) inTahunMulaiTrx,month(datglmulaitransaksi::date)inBulanMulaiTrx
        from customer
        where inkdwilayah in (select wil from wilayah)
    ) b on a.customer_key = b.customer_key
    left join (
        select product_key,chkp
        from produkPPI
    ) c on a.product_key = c.product_key
    group by intahun,inbulan,inkdwilayah,inkdcabang,inkddepo,chkdemployee,chNamaEmp,chkdda,chkdsite,
    chkdcustomer,chNamaCustomer,chkp,inTahunMulaiTrx,inBulanMulaiTrx,loCustomerBaru
    ;

    perform create local temporary table if not exists tempomsetkp
    (
        inkdwilayah int,chketwilayah varchar(255),chkdsite varchar(255),chkdemployee varchar(255),chnamaemployee varchar(255),
        chkp varchar(255),chkdda varchar(255),
        deQtyTarget dec(25,6),deQtyOmset dec(25,6),deRpOmset dec(25,6),
        percentQtyNetto dec(25,6)
    ) on commit preserve rows;

    perform insert into tempomsetkp
    select a.inkdwilayah,chketwilayah,a.chkdsite,a.chkdemployee,chnamaemp,a.chkp,a.chkdda,
    isnull(deQtyTarget,0) qtyTarget,isnull(deQtyOmset,0) qtyOmset,isnull(deRpOmset,0) rpOmset,
    sum(case when (qtyTarget <= 0) or (qtyOmset <= 0) then 0 else qtyOmset/qtyTarget end) percentQtyNetto
    from (
        select distinct inkdwilayah,chketwilayah,chkdemployee,chnamaemp,chkdda,chkdsite,chkp
        from (select distinct inkdwilayah,chketwilayah,chkdda,chkdsite,chkdemployee,chnamaemp from customer) a
        cross join (select distinct chkp from produkPPI) b
        where chkp is not null
    ) a
    left join (
        select inkdwilayah,chkdemployee,chkdda,chkdsite,chkp,
        sum(isnull(deQtyNetto,0)) deQtyOmset,
        sum(isnull(deRpNetto,0)) deRpOmset
        from prelistlt
        where inTipeOms in (2)
        group by inkdwilayah,chkdemployee,chkdda,chkdsite,chkp
    ) b on a.chkdda = b.chkdda and a.chkdsite = b.chkdsite and a.chkp = b.chkp and a.inkdwilayah = b.inkdwilayah
    and a.chkdemployee = b.chkdemployee
    left join (
        select chkp,
        sum(isnull(deQtyNetto,0)) deQtyTarget
        from prelistlt
        where inTipeOms in (0) and chTipeKp in ('KP')
        group by chkp
    ) c on a.chkp = c.chkp
    group by a.inkdwilayah,chketwilayah,a.chkdsite,a.chkdemployee,chnamaemp,a.chkp,a.chkdda,deQtyTarget,deQtyOmset,deRpOmset
    ;

    perform create local temporary table if not exists insomsetkp
    (
        inkdwilayah int,chketwilayah varchar(255),chkdsite varchar(255),chkdemployee varchar(255),chnamaemployee varchar(255),chkp varchar(255),
        deQtyTarget dec(25,6),deQtyOmset dec(25,6),deRpOmset dec(25,6),deRateMultiplier dec(25,6),tarifins dec(25,6)
    ) on commit preserve rows;

    perform insert into insomsetkp
    select inkdwilayah,chketwilayah,chkdsite,chkdemployee,chnamaemployee,chkp,
    isnull(deQtyTarget,0) deQtyTarget1,isnull(deQtyOmset,0) deQtyOmset1,isnull(deRpOmset,0) deRpOmset1,pctQtyNettoMultiplier,
    pctQtyNettoMultiplier * deRpOmset1 tarifins
    from (
        select inkdwilayah,chketwilayah,chkdsite,chkdemployee,chnamaemployee,chkp,sum(deQtyTarget) deQtyTarget,sum(deQtyOmset) deQtyOmset,
        sum(deRpOmset) deRpOmset,sum(percentQtyNetto) percentQtyNetto1,
        case
            -- AE
            when vtipeperiode in (2) and isnull(percentQtyNetto1,0) < 0.80 then 0
            when vtipeperiode in (2) and isnull(percentQtyNetto1,0) < 0.90 then 0.0015
            when vtipeperiode in (2) and isnull(percentQtyNetto1,0) < 1.00 then 0.0035
            when vtipeperiode in (2) and isnull(percentQtyNetto1,0) >= 1.00 then 0.0060

            -- AAM
            when vtipeperiode in (3) and isnull(percentQtyNetto1,0) < 0.80 then 0
            when vtipeperiode in (3) and isnull(percentQtyNetto1,0) < 0.90 then 0.0010
            when vtipeperiode in (3) and isnull(percentQtyNetto1,0) < 1.00 then 0.0020
            when vtipeperiode in (3) and isnull(percentQtyNetto1,0) >= 1.00 then 0.0030
            else 0
        end pctQtyNettoMultiplier
        from tempomsetkp
        group by inkdwilayah,chketwilayah,chkdsite,chkdemployee,chnamaemployee,chkp
    ) b
    ;

    perform create local temporary table if not exists tempomsetkpglobal
    (
        inkdwilayah int,chketwilayah varchar(255),chkdemployee varchar(255),chnamaemployee varchar(255),
        chkdda varchar(255),chkdsite varchar(255),chkp varchar(255),
        deQtyTarget dec(25,6),deQtyOmset dec(25,6),deRpOmset dec(25,6),
        percentQtyNetto dec(25,6)
    ) on commit preserve rows;

    perform insert into tempomsetkpglobal
    select a.inkdwilayah,chketwilayah,a.chkdemployee,chnamaemp,a.chkdda,a.chkdsite,chkp,
    isnull(deQtyTarget,0) qtyTarget,isnull(deQtyOmset,0) qtyOmset,isnull(deRpOmset,0) rpOmset,
    sum(case when (qtyTarget <= 0) or (qtyOmset <= 0) then 0 else qtyOmset/qtyTarget end) percentQtyNetto
    from (
        select distinct inkdwilayah,chketwilayah,chkdda,chkdsite,chkdemployee,chnamaemp from customer
    ) a
    left join (
        select inkdwilayah,chkdemployee,chkdda,chkdsite,
        sum(isnull(deQtyNetto,0)) deQtyOmset,
        sum(isnull(deRpNetto,0)) deRpOmset
        from prelistlt
        where inTipeOms in (2)
        group by inkdwilayah,chkdemployee,chkdda,chkdsite
    ) b on a.chkdda = b.chkdda and a.chkdsite = b.chkdsite
    cross join (
        select sum(isnull(deQtyNetto,0)) deQtyTarget,chkp
        from prelistlt
        where inTipeOms in (1)
        group by chkp
    ) c
    group by a.inkdwilayah,chketwilayah,a.chkdemployee,chnamaemp,a.chkdda,a.chkdsite,chkp,deQtyTarget,deQtyOmset,deRpOmset
    ;

    perform create local temporary table if not exists insomsetkpglobal
    (
        inkdwilayah int,chketwilayah varchar(255),chkdsite varchar(255),chkdemployee varchar(255),chnamaemployee varchar(255),chkp varchar(255),
        deQtyTarget dec(25,6),deQtyOmset dec(25,6),deRpOmset dec(25,6),dePctQtyNettoMultiplier dec(25,6),totalins dec(25,6)
    ) on commit preserve rows;

    perform insert into insomsetkpglobal
    select inkdwilayah,chketwilayah,chkdsite,chkdemployee,chnamaemployee,chkp,
    isnull(deQtyTarget,0) deQtyTarget1,isnull(deQtyOmset,0) deQtyOmset1,isnull(deRpOmset,0) deRpOmset1,pctQtyNettoMultiplier,
    pctQtyNettoMultiplier * deRpOmset1 totalins
    from (
        select inkdwilayah,chketwilayah,chkdsite,chkdemployee,chnamaemployee,chkp,
        sum(deQtyTarget) deQtyTarget,sum(deQtyOmset) deQtyOmset,sum(deRpOmset) deRpOmset,sum(percentQtyNetto) percentQtyNetto1,
        case
            when vtipeperiode in (2) and isnull(percentQtyNetto1,0) < 0.80 then 0
            when vtipeperiode in (2) and isnull(percentQtyNetto1,0) < 0.90 then 0.0007
            when vtipeperiode in (2) and isnull(percentQtyNetto1,0) < 1 then 0.0015
            when vtipeperiode in (2) and isnull(percentQtyNetto1,0) >= 1 then 0.0030

            when vtipeperiode in (3) and isnull(percentQtyNetto1,0) < 0.80 then 0
            when vtipeperiode in (3) and isnull(percentQtyNetto1,0) < 0.90 then 0.0005
            when vtipeperiode in (3) and isnull(percentQtyNetto1,0) < 1 then 0.0010
            when vtipeperiode in (3) and isnull(percentQtyNetto1,0) >= 1 then 0.0015

            else 0
        end pctQtyNettoMultiplier
        from tempomsetkpglobal
        group by inkdwilayah,chketwilayah,chkdsite,chkdemployee,chnamaemployee,chkp
    ) a
    ;

    perform create local temporary table if not exists listlt
    (
        chkdsite varchar(255),inkdwilayah int,chkdemployee varchar(255),chnamaemp varchar(255),
        chkdcustomer varchar(255),chnamacustomer varchar(255),chkp varchar(255),
        inTahunMulaiTrx int,inBulanMulaiTrx int,loCustomerBaru boolean,
        deQtyOmset dec(25,6),deRpOmset dec(25,6)
    ) on commit preserve rows;

    perform insert into listlt
    select chkdsite,inkdwilayah,chkdemployee,chnamaemp,chkdcustomer,chnamacustomer,
    chkp,intahunmulaitrx,inbulanmulaitrx,locustomerbaru,
    sum(deQtyNetto) deQtyOmset,sum(deRpNetto) deRpOmset
    from prelistlt
    where inTipeOms in (2)
    group by chkdsite,inkdwilayah,inkdcabang,inkddepo,chkdemployee,chnamaemp,chkdcustomer,chnamacustomer,
    chkp,intahunmulaitrx,inbulanmulaitrx,locustomerbaru
    ;

    perform create local temporary table if not exists insentiflt
    (
        inkdwilayah int,chkdemployee varchar(255),
        deTarifLt50010 dec(25,6),deTarifLt1050 dec(25,6),deTarifLt50up dec(25,6),totalIns dec(25,6)
    ) on commit preserve rows;

    perform insert into insentiflt
    select inkdwilayah,chkdemployee,
    sum(case when vtipeperiode in (2,3) and isnull(inJumlahLt,0) >= stdInsCA then isnull(inlt50010,0) * 2500::dec(25,6) else 0 end) deTarifLt50010,
    sum(case when vtipeperiode in (2,3) and isnull(inJumlahLt,0) >= stdInsCA then isnull(inlt1050,0) * 10000::dec(25,6) else 0 end) deTarifLt1050,
    sum(case when vtipeperiode in (2,3) and isnull(inJumlahLt,0) >= stdInsCA then isnull(inlt50up,0) * 20000::dec(25,6) else 0 end) deTarifLt50up,
    (deTarifLt50010 + deTarifLt1050 + deTarifLt50up) totalIns
    from (
        select inkdwilayah,chkdemployee,
        count(distinct case when isnull(deRpOmset,0) >= 500000 and isnull(derpomset,0) < 10000000  then chkdcustomer end) inlt50010,
        count(distinct case when isnull(deRpOmset,0) >= 10000000 and isnull(derpomset,0) <= 50000000 then chkdcustomer end) inlt1050,
        count(distinct case when isnull(deRpOmset,0) >  50000000 then chkdcustomer end) inlt50up,
        (inlt50010 + inlt1050 + inlt50up) inJumlahLT
        from listlt
        group by inkdwilayah,chkdemployee
    ) a
    group by inkdwilayah,chkdemployee
    ;

    perform create local temporary table if not exists insentiflb
    (
        inkdwilayah int,chkdemployee varchar(255),chkdda varchar(255),
        deTarifLB0106 dec(25,6),deTarifLB0610 dec(25,6),deTarifLB10up dec(25,6),totalInsLB dec(25,6)
    ) on commit preserve rows;

    perform insert into insentiflb
    select inkdwilayah,chkdemployee,
    sum(case when vtipeperiode in (2,3) and isnull(inJumlahLB,0) >= stdInsNOC then isnull(inlb0106,0) * 10000::dec(25,6) else 0 end) deTarifLB0106,
    sum(case when vtipeperiode in (2,3) and isnull(inJumlahLB,0) >= stdInsNOC then isnull(inlb0610,0) * 20000::dec(25,6) else 0 end) deTarifLB0610,
    sum(case when vtipeperiode in (2,3) and isnull(inJumlahLB,0) >= stdInsNOC then isnull(inlb10up,0) * 50000::dec(25,6) else 0 end) deTarifLB10up,
    (deTarifLB0106 + deTarifLB0610 + deTarifLB10up) totalInsLb
    from (
        select inkdwilayah,chkdemployee,
        count(distinct case when isnull(deRpOmset,0) >= 1000000 and isnull(derpomset,0) < 6000000  then chkdcustomer end) inlb0106,
        count(distinct case when isnull(deRpOmset,0) >= 6000000 and isnull(derpomset,0) <= 10000000 then chkdcustomer end) inlb0610,
        count(distinct case when isnull(deRpOmset,0) >  10000000 then chkdcustomer end) inlb10up,
        (inlb0106 + inlb0610 + inlb10up) inJumlahLB
        from listlt
        where loCustomerBaru = true
        group by inkdwilayah,chkdemployee
    ) a
    group by inkdwilayah,chkdemployee
    ;

    -- Syarat Prestasi Tagih
    perform create local temporary table if not exists piutangbulanan
    (
        inkdwilayah int,chkdemployee varchar(255),chnamaemployee varchar(255),chkdsite varchar(255),
        chkdcustomer varchar(255),chnamacustomer varchar(255),
        deNilaiFaktur dec(25,6),chnofaktur varchar(255),datgljt date,deTarget dec(25,6),deReal dec(25,6)
    ) on commit preserve rows;

    perform insert into piutangbulanan
    select inkdwilayah,chkdemployee,chnamaemp,chkdsite,chkdcustomer,chnamacustomer,
    sum(deNilaiFaktur) deNilaiFaktur,left(chnofaktur,14) chnofaktur1,datgljt,
    sum(deTargetMonth) deTarget,sum(deBayarMonth) deReal
    from lp_tpiutang a
    inner join (
        select customer_key,inkdwilayah,chkdemployee,chnamaemp,chkdsite,chkdcustomer,chnamacustomer from customer
    ) b on a.customer_key = b.customer_key
    where datglcutoff = maxdate and a.customer_key in (select customer_key from customer)
    group by inkdwilayah,chkdemployee,chnamaemp,chkdsite,chkdcustomer,chnamacustomer,chnofaktur1,datgljt
    ;

    perform create local temporary table if not exists hitungsyaratbayar
    (
        inkdwilayah int,chkdsite varchar(255),chkdemployee varchar(255),
        deTarget dec(25,6),deReal dec(25,6),dePercentTagih dec(25,6)
    ) on commit preserve rows;

    perform insert into hitungsyaratbayar
    select inkdwilayah,chkdsite,chkdemployee,
    sum(deTarget) deTarget1,sum(deReal) deReal1,
    case when isnull(deTarget1,0) <= 0 or isnull(deReal1,0) <= 0 then 0 else (deReal1 / deTarget1) end dePercentTagih
    from piutangbulanan
    group by inkdwilayah,chkdsite,chkdemployee
    ;

    perform create local temporary table if not exists insentiffinal
    (
        inkdwilayah int,chkdemployee varchar(255),multiplier dec(25,6),
        dePctTagih dec(25,6),deInsKp dec(25,6),deInsKpGlobal dec(25,6),deInsLt dec(25,6),deInsLb dec(25,6),
        deTotalInsentif dec(25,6)
    ) on commit preserve rows;

    perform insert into insentiffinal
    select a.inkdwilayah,a.chkdemployee,isnull(pctTagihMultiplier,0) pctTagihMultiplier1,isnull(pctTagih,0) pctTagih1,
    pctTagihMultiplier1 * isnull(inskp,0)          calcInsKp,
    pctTagihMultiplier1 * isnull(inskpglobal,0)    calcInsKpGlobal,
    pctTagihMultiplier1 * isnull(inslt,0)          calcInsLt,
    pctTagihMultiplier1 * isnull(inslb,0)          calcInsLb,
    (calcInsKp + calcInsKpGlobal + calcInsLt + calcInsLb) deTotalInsFinal
    from (
        select a.inkdwilayah,a.chkdemployee,
        sum(isnull(totalInsKp,0)) inskp,
        sum(isnull(totalInsKpGlobal,0)) inskpglobal,
        sum(isnull(totalInsLT,0)) inslt,
        sum(isnull(totalInsLB,0)) inslb
        from (
            select distinct inkdwilayah,chkdemployee
            from customer
        ) a
        left join (
            select inkdwilayah,chkdemployee,sum(isnull(tarifins,0)) totalInsKp from insomsetkp
            group by inkdwilayah,chkdemployee
        ) b on a.inkdwilayah = b.inkdwilayah and a.chkdemployee = b.chkdemployee
        left join (
            select inkdwilayah,chkdemployee,sum(isnull(totalins,0)) totalInsKpGlobal from insomsetkpglobal
            group by inkdwilayah,chkdemployee
        ) c on a.inkdwilayah = c.inkdwilayah and a.chkdemployee = c.chkdemployee
        left join (
            select inkdwilayah,chkdemployee,sum(isnull(totalIns,0)) totalInsLT from insentifLT
            group by inkdwilayah,chkdemployee
        ) d on a.inkdwilayah = d.inkdwilayah and a.chkdemployee = d.chkdemployee
        left join (
            select inkdwilayah,chkdemployee,sum(isnull(totalInsLB,0)) totalInsLB from insentifLB
            group by inkdwilayah,chkdemployee
        ) e on a.inkdwilayah = e.inkdwilayah and a.chkdemployee = e.chkdemployee
        group by a.inkdwilayah,a.chkdemployee
    ) a
    left join (
        select inkdwilayah,chkdemployee,
        sum(isnull(dePercentTagih,0)) pctTagih,
        case
            when vtipeperiode in (2,3) and pctTagih < 0.80 then 0
            when vtipeperiode in (2,3) and pctTagih >= 0.80 and pctTagih < 0.90 then 0.80
            when vtipeperiode in (2,3) and pctTagih >= 0.90 and pctTagih < 0.95 then 0.90
            when vtipeperiode in (2,3) and pctTagih >= 0.95 then 1.00
            else 0
        end pctTagihMultiplier
        from hitungsyaratbayar
        group by inkdwilayah,chkdemployee
    ) b on a.inkdwilayah = b.inkdwilayah and a.chkdemployee = b.chkdemployee
    ;

    perform create local temporary table if not exists list_detail
    (
        chnosurat varchar(255),intipe int,intahun int,inbulan int,inperiode int,inpekan int,inkdwilayah int,inkdcabang int,
        inkddepo int,chkdsite varchar(255),inkdtypeins int,chkettypeins varchar(255),chempid varchar(255),chketemp varchar(255),
        chkdcustomer varchar(255),locustomerbaru boolean,chkp varchar(255),chnofaktur varchar(255),datgljt date,
        deqtynetto dec(25,6),derpnetto dec(25,6),detarget dec(25,6),dereal dec(25,6),
        chusercreated varchar(255),dacreated date,chketcustomer varchar(255)
    ) on commit preserve rows;
    -- targetQTY,omsetQTY,omsetRP,null

    /**
        * tipe ins classification
        * 0: Omset KP + LT without target
        * 1: Omset KP only with Target
        * 2: Prestasi Tagih
    */
    perform insert into list_detail
    select nosurat,0 detailTipeIns,vtahun,vbulan,vtipeperiode,0 inpekan,a.inkdwilayah,null inkdcabang,
    null inkddepo,null chkdsite,vtipeperiode,vketemployee,a.chkdemployee chempid,chnamaemp chketemp,
    chkdcustomer,loCustomerBaru,a.chkp,null chnofaktur,null datgljt,
    deQtyNetto deQtyOmset,deRpNetto deRpOmset,null deTarget,null deReal,
    vuser,waktusaatini,chNamaCustomer chketcustomer
    from (
        select inkdwilayah,chkdemployee,chnamaemp,chkdcustomer,locustomerbaru,
        chkp,deQtyOmset deQtyNetto,deRpOmset deRpNetto,chnamacustomer
        from listlt
    ) a
    ;

    perform insert into list_detail
    select nosurat,1 detailTipeIns,vtahun,vbulan,vtipeperiode,0 inpekan,a.inkdwilayah,null inkdcabang,
    null inkddepo,null chkdsite,vtipeperiode,vketemployee,a.chkdemployee chempid,chnamaemp chketemp,
    null chkdcustomer,null loCustomerBaru,chkp,null chnofaktur,null datgljt,
    deqtytarget deTargetQty,deqtyomset deQtyNetto,derpomset deRpNetto,deRateMultiplier deReal,
    vuser,waktusaatini,null chketcustomer
    from insomsetkp a
    inner join (
        select distinct chnamaemp,chkdemployee from customer
    ) b on a.chkdemployee = b.chkdemployee
    ;

    perform insert into list_detail
    select nosurat,1 detailTipeIns,vtahun,vbulan,vtipeperiode,0 inpekan,a.inkdwilayah,null inkdcabang,
    null inkddepo,null chkdsite,vtipeperiode,vketemployee,a.chkdemployee chempid,chnamaemp chketemp,
    null chkdcustomer,null loCustomerBaru,chkp,null chnofaktur,null datgljt,
    deqtytarget deTargetQty,deqtyomset deQtyNetto,derpomset deRpNetto,dePctQtyNettoMultiplier deReal,
    vuser,waktusaatini,null chketcustomer
    from insomsetkpglobal a
    inner join (
        select distinct chnamaemp,chkdemployee from customer
    ) b on a.chkdemployee = b.chkdemployee
    ;

    perform insert into list_detail
    select nosurat,2 detailTipeIns,vtahun,vbulan,vtipeperiode,0 inpekan,inkdwilayah,null inkdcabang,
    null inkddepo,null chkdsite,vtipeperiode,vketemployee,chkdemployee chempid,chnamaemployee chketemp,
    chkdcustomer,null loCustomerBaru,null chkp,chnofaktur,datgljt,
    null deQtyTarget,null deQtyOmset,detarget,dereal,
    vuser,waktusaatini,chNamaCustomer chketcustomer
    from piutangbulanan
    ;

    perform create local temporary table if not exists vinsrekap
    (
        chkdsite varchar(255),inkdwilayah int,chketwilayah varchar(255),inkdcabang int,chketcabang varchar(255),inkddepo int,chketdepo varchar(255),
        inTahun int,inperiode int,inpekan int,inpekantahun int,inbulan int,inkdtypeins int,inkdins int,
        chkdda varchar(255),chkdemployee varchar(255),chketda varchar(255),chketemployee varchar(255),
        deinsentif dec(25,6),chnosurat varchar(255),locurrent int,inkdteamda int,chusercreated varchar(255),dacreated timestamp,intgl int,deinshangus dec(25,6)
    ) on commit preserve rows;

    -- original value
    perform insert into vinsrekap
    select null chkdsite,a.inkdwilayah,a.chketwilayah,null inkdcabang,null chketcabang,null inkddepo,null chketdepo,
    vtahun inTahun,3 periodeBulanan,0 inpekan,0 inpekantahun,vbulan inbulan,vtipeperiode inkdtypeinsemployee,('9'||inkdins)::int inkdins,
    null chkdda,a.chkdemployee,a.chnamaemployee chketda,a.chnamaemployee chketemployee,
    totalIns deInsentif,nosurat chnosurat,vposisi loCurrent,0 inkdteamda,vuser chUserCreated,waktusaatini daCreated,
    null intgl,null deReal
    from (
        select distinct inkdwilayah,chketwilayah,chkdda,chkdemployee,chnamaemp chnamaemployee
        from customer
    ) a
    left join (
        -- omset kp
        select chkdemployee,inkdwilayah,
        (vtipeperiode||600)::int inkdins,sum(tarifins) totalIns
        from insomsetkp
        group by chkdemployee,inkdwilayah

        union all

        -- omset KP global
        select chkdemployee,inkdwilayah,
        (vtipeperiode||601)::int inkdins,sum(totalins)
        from insomsetkpglobal
        group by chkdemployee,inkdwilayah

        union all

        -- LT Omset
        select chkdemployee,inkdwilayah,
        (vtipeperiode||602)::int inkdins,sum(totalins)
        from insentiflt
        group by chkdemployee,inkdwilayah

        union all

        -- LB Omset
        select chkdemployee,inkdwilayah,
        (vtipeperiode||603)::int inkdins,sum(totalinslb)
        from insentiflb
        group by chkdemployee,inkdwilayah

    ) b on a.inkdwilayah = b.inkdwilayah and a.chkdemployee = b.chkdemployee
    ;

    -- original value prestasi tagih
    perform insert into vinsrekap
    select null chkdsite,a.inkdwilayah,a.chketwilayah,null inkdcabang,null chketcabang,null inkddepo,null chketdepo,
    vtahun inTahun,3 periodeBulanan,0 inpekan,0 inpekantahun,vbulan inbulan,vtipeperiode inkdtypeinsemployee,('9'||inkdins)::int inkdins,
    null chkdda,a.chkdemployee,a.chnamaemployee chketda,a.chnamaemployee chketemployee,
    deTarget deInsentif,nosurat chnosurat,vposisi loCurrent,0 inkdteamda,vuser chUserCreated,waktusaatini daCreated,
    null intgl,deReal deInsHangus
    from (
        select distinct inkdwilayah,chketwilayah,chkdda,chkdemployee,chnamaemp chnamaemployee
        from customer
    ) a
    left join (
        select chkdemployee,inkdwilayah,(vtipeperiode||999)::int inkdins,
        sum(deTarget) deTarget,sum(deReal) deReal
        from piutangbulanan
        group by chkdemployee,inkdwilayah
    ) b on a.inkdwilayah = b.inkdwilayah and a.chkdemployee = b.chkdemployee
    ;

    -- calculated value
    perform insert into vinsrekap
    select null chkdsite,a.inkdwilayah,a.chketwilayah,null inkdcabang,null chketcabang,null inkddepo,null chketdepo,
    vtahun inTahun,3 periodeBulanan,0 inpekan,0 inpekantahun,vbulan inbulan,vtipeperiode inkdtypeinsemployee,inkdins,
    null chkdda,a.chkdemployee,a.chnamaemployee chketda,a.chnamaemployee chketemployee,
    deInsFinal deInsentif,nosurat chnosurat,vposisi loCurrent,0 inkdteamda,vuser chUserCreated,waktusaatini daCreated,
    null intgl,null deInsHangus
    from (
        select distinct inkdwilayah,chketwilayah,chkdda,chkdemployee,chnamaemp chnamaemployee
        from customer
    ) a
    left join (
        select inkdwilayah,chkdemployee,depcttagih,deinskp deInsFinal,(vtipeperiode||600)::int inkdins
        from insentiffinal

        union all

        select inkdwilayah,chkdemployee,depcttagih,deinskpglobal,(vtipeperiode||601)::int inkdins
        from insentiffinal

        union all

        select inkdwilayah,chkdemployee,depcttagih,deinslt,(vtipeperiode||602)::int inkdins
        from insentiffinal

        union all

        select inkdwilayah,chkdemployee,depcttagih,deinslb,(vtipeperiode||603)::int inkdins
        from insentiffinal

    ) b on a.inkdwilayah = b.inkdwilayah and a.chkdemployee = b.chkdemployee
    ;

    -- calculated value prestasi tagih
    perform insert into vinsrekap
    select null chkdsite,a.inkdwilayah,a.chketwilayah,null inkdcabang,null chketcabang,null inkddepo,null chketdepo,
    vtahun inTahun,3 periodeBulanan,0 inpekan,0 inpekantahun,vbulan inbulan,vtipeperiode inkdtypeinsemployee,inkdins,
    null chkdda,a.chkdemployee,a.chnamaemployee chketda,a.chnamaemployee chketemployee,
    dePctTagih deInsentif,nosurat chnosurat,vposisi loCurrent,0 inkdteamda,vuser chUserCreated,waktusaatini daCreated,
    null intgl,multiplier deInsHangus
    from (
        select distinct inkdwilayah,chketwilayah,chkdda,chkdemployee,chnamaemp chnamaemployee
        from customer
    ) a
    left join (
        select inkdwilayah,chkdemployee,multiplier,dePctTagih,(vtipeperiode||999)::int inkdins
        from insentiffinal
    ) b on a.inkdwilayah = b.inkdwilayah and a.chkdemployee = b.chkdemployee
    ;

    perform call SPS_Ins_Loging(1,'INSPPI',0,'Start Insert RPT_insaebln202505001 '||waktusaatini,'00000');

    begin
        errCode := '00000';
        perform delete from PPI_tInsTrxDetil
        where inkdwilayah in (select wil from wilayah) and intahun = vtahun and inbulan = vbulan and
        inkdtypeins = vtipeperiode and left(chnosurat,3)::int = left(nosurat,3)::int
        ;

        perform insert into PPI_tInsTrxDetil
        (
            chnosurat,intipe,intahun,inbulan,inperiode,inpekan,
            inkdwilayah,inkdcabang,inkddepo,chkdsite,inkdtypeins,chkettypeins,
            chempid,chketemp,chkdcustomer,locustomerbaru,chkp,chnofaktur,datgljt,
            deqtynetto,derpnetto,detarget,dereal,dacreated,chketcustomer
        )
        select chnosurat,intipe,intahun,inbulan,inperiode,inpekan,
        inkdwilayah,inkdcabang,inkddepo,chkdsite,inkdtypeins,chkettypeins,
        chempid,chketemp,chkdcustomer,locustomerbaru,chkp,chnofaktur,datgljt,
        deqtynetto,derpnetto,detarget,dereal,dacreated,chketcustomer
        from list_detail
        ;

        perform delete from lp_tinsrekap_hrd
        where inkdwilayah in (select wil from wilayah) and intahun = vtahun and inbulan = vbulan and
        inkdtypeins = vtipeperiode and left(chnosurat,3)::int = left(nosurat,3)::int
        ;

        perform insert into lp_tinsrekap_hrd
        (
            chkdsite,inkdwilayah,chketwilayah,inkdcabang,chketcabang,inkddepo,chketdepo,
            inTahun,inperiode,inpekan,inpekantahun,inbulan,inkdtypeins,inkdins,
            chkdda,chkdemployee,chketda,chketemployee,
            deinsentif,chnosurat,locurrent,inkdteamda,chUserCreated,dacreated,intgl,deinshangus
        )
        select chkdsite,inkdwilayah,chketwilayah,inkdcabang,chketcabang,inkddepo,chketdepo,
        inTahun,inperiode,inpekan,inpekantahun,inbulan,inkdtypeins,inkdins,
        chkdda,chkdemployee,chketda,chketemployee,
        deinsentif,chnosurat,locurrent,inkdteamda,chUserCreated,dacreated,intgl,deinshangus
        from vinsrekap
        ;

        EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS errCode := RETURNED_SQLSTATE;
    end;

    if errCode <> '00000' then
        perform rollback;
        perform call SPS_Ins_Loging(1,'INSPPI',0,'Failed RPT_insaebln2025050001'||waktusaatini,errCode);
    else
        perform commit;
        perform call SPS_Ins_Loging(1,'INSPPI',0,'Success RPT_insaebln2025050001'||waktusaatini,errCode);
    end if;

    perform call SPS_Ins_Loging(1,'INSPPI',0,'End Insert RPT_insaebln2025050001'||waktusaatini,'00000');

end if;
end;
$$
-- TODO: change table with del_ to regular table

-- NOTE: Testing pakai call SP berikut:
-- AE, wil 1, thn 2025, bln 6
-- call RPT_insaebln202505001 ('insaebln202505001-Andrew_mai-'||replace(TO_CHAR(CURRENT_TIMESTAMP,'YYYYMMDD-HH24:MI:SS'),':','')||'-testing.csv','wilayah','01','ALL',2,2025,6,1,0,'Andrew_mai','testing',TO_CHAR(CURRENT_TIMESTAMP,'DD-MM-YYYY-HH24:MI:SS'),1,0,0);
