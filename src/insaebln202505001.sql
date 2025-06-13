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

    vEntity varchar(50);
    ketTeam varchar(50);

    stdInsCA int;
    maxdate date;

    vPath varchar(255);
    mysetdir varchar(255);
    vnmfile2 varchar(255);
    vfile2 varchar(255);

begin
if 1=1 then
    vEntity := SELECT chvalue FROM lp_mreportfilter WHERE chkey = 'db';

    nosurat := '180-ARTA-SDKP_AB2-XI-2024'; -- TODO: Change the nomor surat later

    MySetDir := '/dwh/'||vEntity||'/report/rutin/insentif/insaebln202505001/'||vnama||'.csv';

    vPath := SUBSTR(MySetDir,1,INSTR(MySetDir, '/', -1)-1);
    vnmfile2 := vPath||'/'||vuser||'-Loadins-1-'||vreqkey||'-'||vdate||'.txt';
    vFile2 := REPLACE(SUBSTR(vnmfile2,INSTR(vnmfile2, '/', -1)+1),'.txt','');

    waktusaatini := select now();

    vbulan := vperiode;

    vketemployee :=
    case
        when vtipeperiode in (1) 	then 'AE'
        when vtipeperiode in (2) 	then 'AAM'
        when vtipeperiode in (3) 	then 'RBM'
    end;

    vketposisi :=
    case
        when vposisi in (0) 	then 'Last'
        when vposisi in (1) 	then 'Current'
    end;

    ketteam :=
    case
        when vtipeperiode = 1	then 'AE'
        when vtipeperiode = 2	then 'AAM'
        when vtipeperiode = 3	then 'RBM'
    end;

    stdInsCA := 120 * 0.8;

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
            chkdcustomer,chnamacustomer,chkdemployee,cust.chkdda,chNamaEmp,datglmulaitransaksi
            from lp_mcustomer cust
            inner join (
                select distinct a.chkdda,chkdsite,'9'||substring(chkdemployee,2) chkdemployee,chNama chNamaEmp
                from lp_mda a
                inner join PPI_mInsDaLoad b on a.chkdda = b.chkdda
                where chJabatan in (vketemployee)
            ) emp on cust.chkdda = emp.chkdda and cust.chkdsite = emp.chkdsite
            where cust.inkdwilayah in (select wil from wilayah)
            ;
        end if;

        if vdepolp in (2) then
            perform insert into customer
            select customer_key,cust.inkdwilayah,cust.chketwilayah,cust.inkdcabang,cust.chketcabang,cust.inkddepo,cust.chketdepo,cust.chkdsite,
            chkdcustomer,chnamacustomer,chkdemployee,cust.chkdda,chNamaEmp,datglmulaitransaksi
            from lp_mcustomer cust
            inner join (
                select distinct a.chkdda,chkdsite,'9'||substring(chkdemployee,2) chkdemployee,chNama chNamaEmp
                from lp_mda_aarta a
                inner join PPI_mInsDaLoad b on a.chkdda = b.chkdda
                where chJabatan in (vketemployee)
            ) emp on cust.chkdda = emp.chkdda and cust.chkdsite = emp.chkdsite
            where cust.inkdwilayah in (select wil from wilayah)
            ;
        end if;
    end if;

    if vposisi in (0) then
        if vdepolp in (0,1) then
            perform insert into customer
            select customer_key,cust.inkdwilayah,cust.chketwilayah,cust.inkdcabang,cust.chketcabang,cust.inkddepo,cust.chketdepo,cust.chkdsite,
            chkdcustomer,chnamacustomer,chkdemployee,cust.chkdda,datglmulaitransaksi
            from lp_mcustomer_history cust
            inner join (
                select distinct a.chkdda,chkdsite,'9'||substring(chkdemployee,2) chkdemployee,chNama chNamaEmp
                from lp_mda_history a
                inner join PPI_mInsDaLoad b on a.chkdda = b.chkdda
                where chJabatan in (vketemployee)
            ) emp on cust.chkdda = emp.chkdda and cust.chkdsite = emp.chkdsite
            where cust.inkdwilayah in (select wil from wilayah)
            and intahun = vtahunhistory and inbulan = vbulanhistory
            ;
        end if;

        if vdepolp in (2) then
            perform insert into customer
            select customer_key,cust.inkdwilayah,cust.chketwilayah,cust.inkdcabang,cust.chketcabang,cust.inkddepo,cust.chketdepo,cust.chkdsite,
            chkdcustomer,chnamacustomer,chkdemployee,cust.chkdda,datglmulaitransaksi
            from lp_mcustomer_aarta_history cust
            inner join (
                select distinct a.chkdda,chkdsite,'9'||substring(chkdemployee,2) chkdemployee,chNama chNamaEmp
                from lp_mda_aarta_history a
                inner join PPI_mInsDaLoad b on a.chkdda = b.chkdda
                where chJabatan in (vketemployee)
            ) emp on cust.chkdda = emp.chkdda and cust.chkdsite = emp.chkdsite
            where cust.inkdwilayah in (select wil from wilayah)
            and intahun = vtahunhistory and inbulan = vbulanhistory
            ;
        end if;
    end if;

    /*
     ** Tipe Omset Classification (tipeoms) **
     ** 0: target with KP
     ** 1: target without KP
     ** 2: omset KP
     ** continue here...
     */

    perform create local temporary table if not exists prelistlt
    (
        tipeoms int,intahun int,inbulan int,inkdwilayah int,chkdemployee varchar(255),chkdda varchar(255),chkdsite varchar(255),
        chkdcustomer varchar(255),chTipeKp varchar(255),chkp varchar(255),inTahunMulaiTrx int,inBulanMulaiTrx int,
        deQtyNetto dec(25,6),deRpNetto dec(25,6),loCustomerBaru boolean
    ) on commit preserve rows;

    perform insert into prelistlt
    select 0 tipeoms,null,null,null,null,null,null,null,chproduk chTipeKp,chketproduk chkp,null,null,deTarget deQtyTarget,null,null
    from (
        select chketproduk,deTarget,chproduk
        from PPI_mInsTargetLoad
        where chJabatan in (vketemployee) and chproduk in ('KP')
        and intahun = vtahun and inbulan = vbulan
    ) a
    ;

    perform insert into prelistlt
    select 1 tipeoms,null,null,null,null,null,null,null,chproduk chTipeKp,null,null,null,deTarget deQtyTarget,null,null
    from (
        select deTarget,chproduk
        from PPI_mInsTargetLoad
        where chJabatan in (vketemployee) and chproduk in ('T')
        and intahun = vtahun and inbulan = vbulan
    ) a
    ;

    perform insert into prelistlt
    select 2 tipeoms,intahun,inbulan,inkdwilayah,chkdemployee,chkdda,chkdsite,chkdcustomer,null,chkp,inTahunMulaiTrx,inBulanMulaiTrx,
    sum(deqtynetto) deqtynetto,sum(derpnetto) derpnetto,
    case when inTahunMulaiTrx = inTahun and inBulanMulaiTrx = inBulan then 1 else 0 end loCustomerBaru
    from (
        select product_key,customer_key,deqtynetto,derpnetto,intahun,inbulan
        from dm_tjual_mon
        where intahun = vtahun and inbulan = vbulan
    ) a
    inner join (
        select customer_key,inkdwilayah,chkdemployee,chkdsite,chkdcustomer,chkdda,
        year(datglmulaitransaksi::date) inTahunMulaiTrx,month(datglmulaitransaksi::date)inBulanMulaiTrx
        from customer
        where inkdwilayah in (select wil from wilayah)
    ) b on a.customer_key = b.customer_key
    left join (
        select product_key,chkp
        from produkPPI
    ) c on a.product_key = c.product_key
    group by intahun,inbulan,inkdwilayah,chkdemployee,chkdda,chkdsite,chkdcustomer,chkp,inTahunMulaiTrx,inBulanMulaiTrx,loCustomerBaru
    ;

    -- tempomsetkp:
    -- untuk mencari persentase per KP
    perform create local temporary table if not exists tempomsetkp
    (
        inkdwilayah int,chkdemployee varchar(255),chkp varchar(255),chkdda varchar(255),
        deQtyTarget dec(25,6),deQtyOmset dec(25,6),deRpOmset dec(25,6),
        percentQtyNetto dec(25,6)
    ) on commit preserve rows;

    perform insert into tempomsetkp
    select a.inkdwilayah,a.chkdemployee,a.chkp,a.chkdda,
    isnull(deQtyTarget,0) qtyTarget,isnull(deQtyOmset,0) qtyOmset,isnull(deRpOmset,0) rpOmset,
    sum(case when (qtyTarget <= 0) or (qtyOmset <= 0) then 0 else qtyOmset/qtyTarget end) percentQtyNetto
    from (
        select distinct inkdwilayah,chkdemployee,chkdda,chkdsite,chkp
        from (select distinct inkdwilayah,chkdda,chkdsite,chkdemployee from customer) a
        cross join (select distinct chkp from produkPPI) b
    ) a
    left join (
        select inkdwilayah,chkdemployee,chkdda,chkdsite,chkp,
        sum(isnull(deQtyNetto,0)) deQtyOmset,
        sum(isnull(deRpNetto,0)) deRpOmset
        from prelistlt
        where tipeoms in (2)
        group by inkdwilayah,chkdemployee,chkdda,chkdsite,chkp
    ) b on a.chkdda = b.chkdda and a.chkdsite = b.chkdsite and a.chkp = b.chkp and a.inkdwilayah = b.inkdwilayah
    and a.chkdemployee = b.chkdemployee
    left join (
        select chkp,
        sum(isnull(deQtyNetto,0)) deQtyTarget
        from prelistlt
        where tipeoms in (0) and chTipeKp in ('KP')
        group by chkp
    ) c on a.chkp = c.chkp
    group by a.inkdwilayah,a.chkdemployee,a.chkp,a.chkdda,deQtyTarget,deQtyOmset,deRpOmset
    ;

    perform create local temporary table if not exists insomsetkp
    (
        inkdwilayah int,chkdemployee varchar(255),chkp varchar(255),
        tarifins dec(25,6)
    ) on commit preserve rows;

    perform insert into insomsetkp
    select inkdwilayah,chkdemployee,chkp,
    case
        when isnull(percentQtyNetto,0) < 0.80 then 0
        when isnull(percentQtyNetto,0) < 0.90 then 0.0015 * isnull(deRpOmset,0)
        when isnull(percentQtyNetto,0) < 1 then 0.0035 * isnull(deRpOmset,0)
        when isnull(percentQtyNetto,0) >= 1 then 0.0060 * isnull(deRpOmset,0)
        else 0
    end tarifins
    from tempomsetkp
    ;

    perform create local temporary table if not exists tempomsetkpglobal
    (
        inkdwilayah int,chkdemployee varchar(255),
        deQtyTarget dec(25,6),deQtyOmset dec(25,6),deRpOmset dec(25,6),
        percentQtyNetto dec(25,6)
    ) on commit preserve rows;

    perform insert into tempomsetkpglobal
    select inkdwilayah,chkdemployee,
    isnull(deQtyTarget,0) qtyTarget,isnull(deQtyOmset,0) qtyOmset,isnull(deRpOmset,0) rpOmset,
    sum(case when (qtyTarget <= 0) or (qtyOmset <= 0) then 0 else qtyOmset/qtyTarget end) percentQtyNetto
    from (
        select distinct chkdda,chkdsite from customer
    ) a
    left join (
        select inkdwilayah,chkdemployee,chkdda,chkdsite,
        sum(isnull(deQtyNetto,0)) deQtyOmset,
        sum(isnull(deRpNetto,0)) deRpOmset
        from prelistlt
        where tipeoms in (2)
        group by inkdwilayah,chkdemployee,chkdda,chkdsite
    ) b on a.chkdda = b.chkdda and a.chkdsite = b.chkdsite
    cross join (
        select sum(isnull(deQtyNetto,0)) deQtyTarget
        from prelistlt
        where tipeoms in (1)
    ) c
    group by inkdwilayah,chkdemployee,deQtyTarget,deQtyOmset,deRpOmset
    ;

    perform create local temporary table if not exists insomsetkpglobal
    (
        inkdwilayah int,chkdemployee varchar(255),
        totalins dec(25,6)
    ) on commit preserve rows;

    perform insert into insomsetkpglobal
    select inkdwilayah,chkdemployee,
    case
        when isnull(percentQtyNetto,0) < 0.80 then 0
        when isnull(percentQtyNetto,0) < 0.90 then 0.0007 * isnull(deRpOmset,0)
        when isnull(percentQtyNetto,0) < 1 then 0.0015 * isnull(deRpOmset,0)
        when isnull(percentQtyNetto,0) >= 1 then 0.0030 * isnull(deRpOmset,0)
        else 0
    end totalins
    from tempomsetkpglobal
    ;

    perform create local temporary table if not exists listlt
    (
        inkdwilayah int,chkdsite varchar(255),chkdemployee varchar(255),chkdda varchar(255),chkdcustomer varchar(255),
        inTahunMulaiTrx int,inBulanMulaiTrx int,loCustomerBaru boolean,
        deRpOmset dec(25,6)
    ) on commit preserve rows;

    perform insert into listlt
    select inkdwilayah,a.chkdsite,chkdemployee,a.chkdda,chkdcustomer,
    inTahunMulaiTrx,inBulanMulaiTrx,loCustomerBaru,
    isnull(deRpOmset,0) rpOmset
    from (
        select distinct chkdda,chkdsite from customer
    ) a
    left join (
        select inkdwilayah,chkdemployee,chkdda,chkdsite,chkdcustomer,inTahunMulaiTrx,inBulanMulaiTrx,loCustomerBaru,
        sum(isnull(deRpNetto,0)) deRpOmset
        from prelistlt
        where tipeoms in (2)
        group by inkdwilayah,chkdemployee,chkdda,chkdsite,chkdcustomer,inTahunMulaiTrx,inBulanMulaiTrx,loCustomerBaru
    ) b on a.chkdda = b.chkdda and a.chkdsite = b.chkdsite
    group by inkdwilayah,a.chkdsite,chkdemployee,a.chkdda,chkdcustomer,inTahunMulaiTrx,inBulanMulaiTrx,deRpOmset,locustomerbaru
    ;

    perform create local temporary table if not exists insentiflt
    (
        inkdwilayah int,chkdemployee varchar(255),chkdda varchar(255),
        deTarifLt50010 dec(25,6),deTarifLt1050 dec(25,6),deTarifLt50up dec(25,6),totalIns dec(25,6)
    ) on commit preserve rows;

    perform insert into insentiflt
    select inkdwilayah,chkdemployee,chkdda,
    sum(case when isnull(inJumlahLt,0) >= stdInsCA then isnull(inlt50010,0) * 2500::dec(25,6) else 0 end) deTarifLt50010,
    sum(case when isnull(inJumlahLt,0) >= stdInsCA then isnull(inlt1050,0) * 10000::dec(25,6) else 0 end) deTarifLt1050,
    sum(case when isnull(inJumlahLt,0) >= stdInsCA then isnull(inlt50up,0) * 20000::dec(25,6) else 0 end) deTarifLt50up,
    (deTarifLt50010 + deTarifLt1050 + deTarifLt50up) totalIns
    from (
        select inkdwilayah,chkdemployee,chkdda,
        count(distinct case when isnull(deRpOmset,0) >= 500000 and isnull(derpomset,0) < 10000000  then chkdcustomer end) inlt50010,
        count(distinct case when isnull(deRpOmset,0) >= 10000000 and isnull(derpomset,0) <= 50000000 then chkdcustomer end) inlt1050,
        count(distinct case when isnull(deRpOmset,0) >  50000000 then chkdcustomer end) inlt50up,
        (inlt50010 + inlt1050 + inlt50up) inJumlahLT
        from listlt
        group by inkdwilayah,chkdemployee,chkdda
    ) a
    group by inkdwilayah,chkdemployee,chkdda
    ;

    perform create local temporary table if not exists insentiflb
    (
        inkdwilayah int,chkdemployee varchar(255),chkdda varchar(255),
        deTarifLB0106 dec(25,6),deTarifLB0610 dec(25,6),deTarifLB10up dec(25,6),totalInsLB dec(25,6)
    ) on commit preserve rows;

    perform insert into insentiflb
    select inkdwilayah,chkdemployee,chkdda,
    sum(case when isnull(inJumlahLB,0) >= 10 then isnull(inlb0106,0) * 10000::dec(25,6) else 0 end) deTarifLB0106,
    sum(case when isnull(inJumlahLB,0) >= 10 then isnull(inlb0610,0) * 20000::dec(25,6) else 0 end) deTarifLB0610,
    sum(case when isnull(inJumlahLB,0) >= 10 then isnull(inlb10up,0) * 50000::dec(25,6) else 0 end) deTarifLB10up,
    (deTarifLB0106 + deTarifLB0610 + deTarifLB10up) totalInsLb
    from (
        select inkdwilayah,chkdemployee,chkdda,
        count(distinct case when inTahunMulaiTrx = vtahun and inBulanMulaiTrx = vbulan and isnull(deRpOmset,0) >= 1000000 and isnull(derpomset,0) < 6000000  then chkdcustomer end) inlb0106,
        count(distinct case when inTahunMulaiTrx = vtahun and inBulanMulaiTrx = vbulan and isnull(deRpOmset,0) >= 6000000 and isnull(derpomset,0) <= 10000000 then chkdcustomer end) inlb0610,
        count(distinct case when inTahunMulaiTrx = vtahun and inBulanMulaiTrx = vbulan and isnull(deRpOmset,0) >  10000000 then chkdcustomer end) inlb10up,
        (inlb0106 + inlb0610 + inlb10up) inJumlahLB
        from listlt
        group by inkdwilayah,chkdemployee,chkdda
    ) a
    group by inkdwilayah,chkdemployee,chkdda
    ;

    -- Syarat Prestasi Tagih
    perform create local temporary table if not exists piutangbulanan
    (
        inkdwilayah int,chkdemployee varchar(255),chkdsite varchar(255),chkdda varchar(255),chkdcustomer varchar(255),
        deNilaiFaktur dec(25,6),chnofaktur varchar(255),datgljt date,deTarget dec(25,6),deReal dec(25,6)
    ) on commit preserve rows;

    perform insert into piutangbulanan
    select inkdwilayah,chkdemployee,chkdsite,chkdda,chkdcustomer,
    sum(deNilaiFaktur) deNilaiFaktur,left(chnofaktur,14) chnofaktur1,datgljt,
    sum(deTargetMonth) deTarget,sum(deBayarMonth) deReal
    from lp_tpiutang a
    inner join (
        select customer_key,inkdwilayah,chkdemployee,chkdsite,chkdda,chkdcustomer from customer
    ) b on a.customer_key = b.customer_key
    where datglcutoff = maxdate and a.customer_key in (select customer_key from customer)
    group by inkdwilayah,chkdemployee,chkdsite,chkdda,chkdcustomer,chnofaktur1,datgljt
    ;

    perform create local temporary table if not exists hitungsyaratbayar
    (
        inkdwilayah int,chkdsite varchar(255),chkdemployee varchar(255),chkdda varchar(255),
        deTarget dec(25,6),deReal dec(25,6),dePercentTagih dec(25,6)
    ) on commit preserve rows;

    perform insert into hitungsyaratbayar
    select inkdwilayah,chkdsite,chkdemployee,chkdda,
    sum(deTarget) deTarget1,sum(deReal) deReal1,
    case when isnull(deTarget1,0) <= 0 or isnull(deReal1,0) <= 0 then 0 else (deReal1 / deTarget1) end dePercentTagih
    from piutangbulanan
    group by inkdwilayah,chkdsite,chkdemployee,chkdda
    ;

    perform create local temporary table if not exists insentiffinal
    (
        inkdwilayah int,chkdemployee varchar(255),deTotalInsentif dec(25,6)
    ) on commit preserve rows;

    perform insert into insentiffinal
    select a.inkdwilayah,a.chkdemployee,
    case
        when pctTagih < 0.80 then 0
        when pctTagih >= 0.80 and pctTagih < 0.90 then 0.80 * totalInsFinal
        when pctTagih >= 0.90 and pctTagih < 0.95 then 0.90 * totalInsFinal
        when pctTagih >= 0.95 then 0.80 * totalInsFinal
        else 0
    end deTotalInsFinal
    from (
        select a.inkdwilayah,a.chkdemployee,
        sum(isnull(totalInsKp,0) + isnull(totalInsKpGlobal,0) + isnull(totalInsLT,0) + isnull(totalInsLB,0)) totalInsFinal
        from (
            select distinct inkdwilayah,chkdemployee from customer
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
        select inkdwilayah,chkdemployee,sum(isnull(dePercentTagih,0)) pctTagih from hitungsyaratbayar
        group by inkdwilayah,chkdemployee
    ) b on a.inkdwilayah = b.inkdwilayah and a.chkdemployee = b.chkdemployee
    ;

    perform create local temporary table if not exists vinsrekap2
    (
        chnosurat varchar(255),intipe int,intahun int,inbulan int,inperiode int,inpekan int,inkdwilayah int,inkdcabang int,
        inkddepo int,chkdsite varchar(255),inkdtypeins int,chkettypeins varchar(255),chempid varchar(255),chketemp varchar(255),
        chkdcustomer varchar(255),locustomerbaru boolean,chkp varchar(255),chnofaktur varchar(255),datgljt date,
        deqtynetto dec(25,6),derpnetto dec(25,6),detarget dec(25,6),dereal dec(25,6)
    ) on commit preserve rows;

--     perform insert into vinsrekap2
--     select nosurat,0 detailTipeIns,vtahun,vbulan,vtipeperiode,0 inpekan,inkdwilayah,inkdcabang,
--     inkddepo,chkdsite,vtipeperiode,vketemployee,chkdemployee chempid,chnamaemp chketemp,
--     chkdcustomer,loCustomerBaru
--     from (
--         select distinct * from customer
--     ) a
--     left join (
--         select
--     ) b on

end if;
end;
$$

--call RPT_insaebln202505001 ('insaebln202505001-Andrew_mai-'||replace(TO_CHAR(CURRENT_TIMESTAMP,'YYYYMMDD-HH24:MI:SS'),':','')||'-testing.csv','wilayah','01','ALL',1,2025,5,1,0,'Andrew_mai','testing',TO_CHAR(CURRENT_TIMESTAMP,'DD-MM-YYYY-HH24:MI:SS'),1,0,0);
