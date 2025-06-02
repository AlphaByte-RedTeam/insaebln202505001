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
	vEntity varchar(255);
	MySetDir varchar(255);

	damaxdatemin6 date;

	periodlast varchar(255);
	vtahunlalu int;
	datglawal date;
	datglakhir date;

	vtahunlast int;
	vbulanlast int;

	file_name varchar(255);
	set_cmd varchar(255);
	unset_cmd varchar(255);

	vnmfile1 varchar(255);
	vnmfile2 varchar(255);
	vnmfile3 varchar(255);
	vnmfile4 varchar(255);

	nosurat varchar(255);

	vinstagihan int;
	ketteam varchar(255);
	pathkonsol varchar(255);

	minharikerjatmo date;
	maxharikerjatmo date;

	maxdate date;
	maxdateagen date;

	vsepup int;
	vbulan int;

	charea varchar(255);
	vketemployee varchar(255);
	vketposisi varchar(255);
	waktusaatini timestamp;

	vPath varchar(255);
	vFile varchar(255);
	vFile2 varchar(255);
	vFile3 varchar(255);
	vFile4 varchar(255);

begin
if 1=1 then
    vEntity := SELECT chvalue FROM lp_mreportfilter WHERE chkey = 'db';

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
		periodlast := select max(intahun||right('00'||inbulan,2)) from lp_mdepo_history;
		vtahunlast := left(periodlast,4)::int;
		vbulanlast := right(periodlast,2)::int;
	end if;

    MySetDir := '/dwh/'||vEntity||'/report/rutin/insentif/insaebln202505001/'||vnama||'.csv';

	vPath := SUBSTR(MySetDir,1,INSTR(MySetDir, '/', -1)-1);
	vFile := REPLACE(SUBSTR(MySetDir,INSTR(MySetDir, '/', -1)+1),'.csv','');

	vnmfile1 := vPath||'/'||vnama;
	vnmfile2 := vPath||'/'||vuser||'-Loadins-1-'||vreqkey||'-'||vdate||'.txt';
	vFile2 := REPLACE(SUBSTR(vnmfile2,INSTR(vnmfile2, '/', -1)+1),'.txt','');
	vnmfile3 := vPath||'/'||vuser||'-Loadins-2-'||vreqkey||'-'||vdate||'.txt';
	vFile3 := REPLACE(SUBSTR(vnmfile3,INSTR(vnmfile3, '/', -1)+1),'.txt','');
-- 	vnmfile4 := '/dwh/'||vEntity||'/report/rutin/insentif/konsolins/insdahrn/'||replace(vnama,'.csv','-'||vwilayah||'-'||(case when vdepolp in (0,1) then 'depo' else 'agen' end)||'.csv');
-- 	vFile4 := REPLACE(SUBSTR(vnmfile4,INSTR(vnmfile4, '/', -1)+1),'.csv','');

    perform create local temporary table if not exists wilayah (wil int) on commit preserve rows;

	perform insert into wilayah
	select split_part(vwilayah, '-', period_key)::int from dual
	cross join (select period_key from lp_mperiod where period_key <= regexp_count(vwilayah, '-')+1) a
	;

	perform create local temporary table if not exists vtempwaktu
	(
		period_key int,intahun int,inbulan int,datgl date,inpekansm131 int
	) on commit preserve rows;

    perform insert into vtempwaktu
	select period_key,intahun,inbulan,datgl,inpekansm131 from lp_mperiod
    where intahun = vtahun and inbulan = vbulan;

	perform create local temporary table if not exists employee
	(
	    da_key int,chKdDa varchar(255),inKdWilayah int,chKetWilayah varchar(255),inKdCabang int,chKetCabang varchar(255),
	    inKdDepo int,chKetDepo varchar(255),chJabatan varchar(255),chKdSite varchar(255),inKdType int,
        chKdEmployee varchar(255),chNamaEmployee varchar(255)
    ) on commit preserve rows;

    perform insert into employee
    select a.da_key,a.chkdda,inkdwilayah,chketwilayah,inkdcabang,chketcabang,
    inkddepo,chketdepo,chjabatan,chkdsite,
    case
        when vtipeperiode = 1 then 1
        when vtipeperiode = 2 then 2
        when vtipeperiode = 3 then 3
    end inkdtype,
    substring(chkdemployee,2),chNama chNamaEmployee
    from lp_mda a
    left join (
        select chkdda,chjabatan,chNama from PPI_mInsDALoad
    ) b on a.chkdda = b.chkdda
    where chjabatan = 'AE'
    ;

    perform create local temporary table if not exists produkPPI
    (
        product_key int,inkdkonvbesarid int,chkdbarang varchar(255),chkp varchar(255),chklasifikasi varchar(255),chkpprodukmodel varchar(255),
        chpt varchar(255),chflagitemcustom varchar(255),chflagaktif varchar(255),itemfestive varchar(255)
    ) on commit preserve rows;

    perform insert into produkPPI
    select product.product_key,product.inkdkonvbesarid,product.chkdbarang,chkp,chklasifikasi,chkpprodukmodel,
    chpt,chflagitemcustom,chflagaktif,chflagitemfestive
    from lp_mproduct product
    inner join (
        select chkdbarang,chkp,chklasifikasi,chkpprodukmodel,chpt,chflagitemfestive
        chflagitemcustom,chflagaktif,chflagitemfestive
        from PPI_mItem
    ) b on product.chkdbarang = b.chkdbarang
    ;

    perform create local temporary table if not exists customer
    (
        customer_key int,inkdwilayah int,chketwilayah varchar(255),inkdcabang int,chketcabang varchar(255),
        inkddepo int,chketdepo varchar(255),chkdsite varchar(255),chkdcustomer varchar(255),chnamacustomer varchar(255),
        chkdemployee varchar(255),chkdda varchar(255),datglmulaitransaksi date
    ) on commit preserve rows;

    perform insert into customer
    select customer_key,cust.inkdwilayah,cust.chketwilayah,cust.inkdcabang,cust.chketcabang,cust.inkddepo,cust.chketdepo,cust.chkdsite,
    chkdcustomer,chnamacustomer,chkdemployee,cust.chkdda,datglmulaitransaksi
    from lp_mcustomer cust
    inner join employee emp on cust.chkdda = emp.chkdda and cust.chkdsite = emp.chkdsite
    where cust.inkdwilayah in (select wil from wilayah)
    ;

    perform create local temporary table if not exists prelistlt
    (
        tipeoms int,inkdwilayah int,chkdsite varchar(255),deqtynetto dec(25,6),derpnetto dec(25,6),
        intahun int,inbulan int,chkdemployee varchar(255),chkp varchar(255)
    ) on commit preserve rows;

    perform insert into prelistlt
    select 0 tipeoms,inkdwilayah,chkdsite,
    case when isnull(inkdkonvbesarid,0) = 0 then 0 else sum(tgtQtyThLalu/inkdkonvbesarid) end deqtynetto,
    sum(tgtOmsThLalu) derpnetto,
    intahun,inbulan,chkdemployee,chkp
    from (
        select customer_key,product_key,intahun,inbulan,sum(deqtynetto/12.00) tgtQtyThLalu,sum(derpnetto/12.00) tgtOmsThLalu from dm_tjual_mon
        group by customer_key,product_key,intahun,inbulan
    ) a
    inner join (
        select product_key,inkdkonvbesarid,chkdbarang,chkp from produkPPI
    ) b on a.product_key = b.product_key
    inner join (
        select customer_key,inkdwilayah,chkdsite,chkdemployee
        from customer
    ) c on a.customer_key = c.customer_key
    where intahun = vtahun and inbulan = vbulan
    group by inkdwilayah,chkdsite,inkdkonvbesarid,intahun,inbulan,chkdemployee,chkp
    ;

end if;
end;
$$

--call RPT_insaebln202505001 ('insaebln202505001-Andrew_mai-'||replace(TO_CHAR(CURRENT_TIMESTAMP,'YYYYMMDD-HH24:MI:SS'),':','')||'-testing.csv','wilayah','01','ALL',1,'2025',1,0,0,'Andrew_mai','testing',TO_CHAR(CURRENT_TIMESTAMP,'DD-MM-YYYY-HH24:MI:SS'),1,0,0);
