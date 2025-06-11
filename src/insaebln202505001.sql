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

begin
if 1=1 then
    vEntity := SELECT chvalue FROM lp_mreportfilter WHERE chkey = 'db';

    nosurat := '180-ARTA-SDKP_AB2-XI-2024'; -- TODO: Change the nomor surat later

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
                where chjabatan = 'AE'
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
                where chjabatan = 'AE'
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
                where chjabatan = 'AE'
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
                where chjabatan = 'AE'
            ) emp on cust.chkdda = emp.chkdda and cust.chkdsite = emp.chkdsite
            where cust.inkdwilayah in (select wil from wilayah)
            and intahun = vtahunhistory and inbulan = vbulanhistory
            ;
        end if;
    end if;

    /*
     ** Tipe Omset Classification (tipeoms) **
     ** 0: insentif achieve sales vs target tahun lalu
     ** 2: insentif achieve sales vs target tahun & bulan berjalan
     ** continue here...
     */

    perform create local temporary table if not exists prelistlt
    (
        tipeoms int,inkdwilayah int,chkdemployee varchar(255),chkdsite varchar(255),chkdcustomer varchar(255),chkp varchar(255),
        deQtyNetto dec(25,6),deRpNetto dec(25,6)
    ) on commit preserve rows;

    perform insert into prelistlt
    select 0 tipeoms,null,null,null,null,chketproduk chkp,deTarget deQtyTarget,null
    from (
        select chketproduk,deTarget
        from PPI_mInsTargetLoad
        where chjabatan = 'AE' and chproduk in ('KP','T')
        and intahun = vtahun and inbulan = vbulan
    ) a
    ;

    perform insert into prelistlt
    select 2 tipeoms,inkdwilayah,chkdemployee,chkdsite,chkdcustomer,chkp,
    sum(deqtynetto) deqtynetto,sum(derpnetto) derpnetto
    from (
        select product_key,customer_key,deqtynetto,derpnetto
        from dm_tjual_mon
        -- TODO: use hardcoded value in inBulan for testing purpose, change later
        where intahun = vtahun and inbulan = 1 --inbulan = vbulan
    ) a
    inner join (
        select customer_key,inkdwilayah,chkdemployee,chkdsite,chkdcustomer
        from customer
        where inkdwilayah in (select wil from wilayah)
    ) b on a.customer_key = b.customer_key
    left join (
        select product_key,chkp
        from produkPPI
    ) c on a.product_key = c.product_key
    group by inkdwilayah,chkdemployee,chkdsite,chkdcustomer,chkp
    ;

-- cross join dengan produkPPI
    -- tempomsetkp:
    -- untuk mencari persentase per KP
    perform create local temporary table if not exists tempomsetkp
    (
        inkdwilayah int,chkdemployee varchar(255),chkp varchar(255),chkdcustomer varchar(255),
        deQtyTarget dec(25,6),deQtyOmset dec(25,6),deRpOmset dec(25,6),
        percentQtyNetto dec(25,6)
    ) on commit preserve rows;

    perform insert into tempomsetkp
    select inkdwilayah1,chkdemployee,chkp,chkdcustomer,
    isnull(deQtyTarget,0) qtyTarget,isnull(deQtyOmset,0) qtyOmset,isnull(deRpOmset,0) rpOmset,
    sum(case when (qtyTarget <= 0) or (deQtyOmset <= 0) then 0 else deQtyOmset/qtyTarget end) percentQtyNetto
    from (
        select case when tipeoms in (2) then inkdwilayah end inkdwilayah1,chkdemployee,chkp,chkdcustomer,
        sum(case when tipeoms in (0) then isnull(deQtyNetto,0) end) deQtyTarget,
        sum(case when tipeoms in (2) then isnull(deQtyNetto,0) end) deQtyOmset,
        sum(case when tipeoms in (2) then isnull(deRpNetto,0) end) deRpOmset
        from prelistlt where tipeoms in (0,2)
        group by tipeoms,inkdwilayah,chkdemployee,chkdcustomer,chkp
    ) a
    group by inkdwilayah1,chkdemployee,chkdcustomer,chkp,deQtyTarget,deQtyOmset,deRpOmset
    ;

    perform create local temporary table if not exists insomsetkp
    (
        inkdwilayah int,chkdemployee varchar(255),chkp varchar(255),chkdcustomer varchar(255),
        tarifins dec(25,6)
    ) on commit preserve rows;

    perform insert into insomsetkp
    select inkdwilayah,chkdemployee,chkp,chkdcustomer,
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
    select inkdwilayah1,chkdemployee,
    isnull(deQtyTarget,0) qtyTarget,isnull(deQtyOmset,0) qtyOmset,isnull(deRpOmset,0) rpOmset,
    sum(case when (qtyTarget <= 0) or (deQtyOmset <= 0) then 0 else deQtyOmset/qtyTarget end) percentQtyNetto
    from (
        select case when tipeoms in (2) then inkdwilayah end inkdwilayah1,chkdemployee,
        sum(case when tipeoms in (0) then isnull(deQtyNetto,0) end) deQtyTarget,
        sum(case when tipeoms in (2) then isnull(deQtyNetto,0) end) deQtyOmset,
        sum(case when tipeoms in (2) then isnull(deRpNetto,0) end) deRpOmset
        from prelistlt where tipeoms in (0,2)
        group by tipeoms,inkdwilayah,chkdemployee
    ) a
    group by inkdwilayah1,chkdemployee,deQtyTarget,deQtyOmset,deRpOmset
    ;

--     perform create local temporary table if not exists insomsetkpglobal
--     (
--         inkdwilayah int,chkdemployee varchar(255),chkdcustomer varchar(255),
--         totalins dec(25,6)
--     ) on commit preserve rows;
--
--     perform insert into insomsetkpglobal
--     select inkdwilayah,chkdemployee,chkdcustomer,
--     case
--         when isnull(percentQtyNetto,0) < 0.80 then 0
--         when isnull(percentQtyNetto,0) < 0.90 then 0.0007 * isnull(deRpNettoCurr,0)
--         when isnull(percentQtyNetto,0) < 1 then 0.0015 * isnull(deRpNettoCurr,0)
--         when isnull(percentQtyNetto,0) >= 1 then 0.0030 * isnull(deRpNettoCurr,0)
--         else 0
--     end totalins
--     from tempomsetkp
--     ;
--
--     perform create local temporary table if not exists listlt
--     (
--         tipeoms int,inkdwilayah int,chkdemployee varchar(255),chkdsite varchar(255),chkdcustomer varchar(255)
--     ) on commit preserve rows;
--
--     perform insert into listlt
--     select tipeoms,inkdwilayah,chkdemployee,chkdsite,chkdcustomer
--     from prelistlt;

end if;
end;
$$

--call RPT_insaebln202505001 ('insaebln202505001-Andrew_mai-'||replace(TO_CHAR(CURRENT_TIMESTAMP,'YYYYMMDD-HH24:MI:SS'),':','')||'-testing.csv','wilayah','01','ALL',1,2025,5,1,0,'Andrew_mai','testing',TO_CHAR(CURRENT_TIMESTAMP,'DD-MM-YYYY-HH24:MI:SS'),1,0,0);
