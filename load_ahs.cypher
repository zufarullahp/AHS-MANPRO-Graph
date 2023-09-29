load csv with headers from "file:///LAPORAN-HARIAN-ATLAS-COPCO.csv" as row 
create (r:Report{Name:row.`Name`})
set 
// r.2=row.`2`,
r.DESKRIPSI_KENDALA=row.`DESKRIPSI / KENDALA`,
r.FOTO_VIDEO_PEKERJAAN=row.`FOTO/VIDEO PEKERJAAN`,
r.Email=row.`Email`,
r.FOTO_VIDEO_PEKERJAAN3=row.`FOTO/VIDEO PEKERJAAN3`,
r.FOTO_VIDEO_PEKERJAAN2=row.`FOTO/VIDEO PEKERJAAN2`,
r.JUMLAH_TENAGA_KERJA2=row.`JUMLAH TENAGA KERJA2`,
r.JUMLAH_TENAGA_KERJA3=row.`JUMLAH TENAGA KERJA3`,
r.MATERIAL_2=row.`MATERIAL (JIKA ADA)2`,
r.QUANTITY_PEKERJAAN2=row.`QUANTITY PEKERJAAN2`,
r.PERALATAN_2=row.`PERALATAN (JIKA ADA)2`,
r.PERALATAN_3=row.`PERALATAN (JIKA ADA)3`,
r.QUANTITY_PEKERJAAN3=row.`QUANTITY PEKERJAAN3`,
r.QUANTITY_PEKERJAAN=row.`QUANTITY PEKERJAAN`,
r.MATERIAL_3=row.`MATERIAL (JIKA ADA)3`,
r.ITEM_PEKERJAAN3=row.`ITEM PEKERJAAN3`,
r.ITEM_PEKERJAAN2=row.`ITEM PEKERJAAN2`,
r.PERALATAN=row.`PERALATAN (JIKA ADA)`,
r.Language=row.`Language`,
r.ID=row.`ID`,
r.Completion_time=row.`Completion time`,
r.SATUAN3=row.`SATUAN3`,
r.SATUAN=row.`SATUAN`,
r.SATUAN2=row.`SATUAN2`,
r.MATERIAL=row.`MATERIAL (JIKA ADA)`,
r.TANGGAL_LAPORAN=row.`TANGGAL LAPORAN`,
r.Column1=row.`Column1`,
r.DESKRIPSI_KENDALA2=row.`DESKRIPSI / KENDALA2`,
r.DESKRIPSI_KENDALA3=row.`DESKRIPSI / KENDALA3`,
r.ITEM_PEKERJAAN=row.`ITEM PEKERJAAN`,
r.JUMLAH_TENAGA_KERJA=row.`JUMLAH TENAGA KERJA`,
r.Start_tim=row.`Start time`;


MATCH (n:Report)
where n.FOTO_VIDEO_PEKERJAAN is not null 
create (v:Content{link:n.FOTO_VIDEO_PEKERJAAN})
create (n)-[:CONTENT]->(v);

match (n:Report)-[c:CONTENT]->(v:Content)
where v.link contains ';'  
    with  n,c,v 
    with n, c, split(v.link,';') as q
    unwind(q) as url 
    create (l:URL{url:url})
    create (l)<-[:HAS_URL]-(n);

match (c:Content) detach delete (c) ;

MATCH (n:Report) 
with  split(n.ITEM_PEKERJAAN,' ') as text,n
unwind range(0,size(text)-2) as i 
merge (w1:Item{text:text[i]})
merge (w2:Item{text:text[i+1]})
merge (w1)-[r:NEXT_DESC]->(w2)
on create set r.count=1
on match set r.count=r.count+1
merge (w1)<-[:SHORT_DESC]-(n)
merge (w2)<-[:SHORT_DESC]-(n);


MATCH (e:Report) 
return e , n.Start_tim as dt order by dt ASC 
WITH collect(e) AS events
CALL apoc.nodes.link(events, "NEXT")
RETURN count(*);


load csv with headers from "file:///Book.csv" as row
WITH row where row.ITEMS is not null and row.ITEMS <>''
merge (r:Activity{name:row.`ITEMS`})
on create set 
r.index_wbs=row.N0
;


match (n:Activity{name:'PEKERJAAN PERANCANGAN'}) set n:Main_Activity;
match (n:Activity{name:'PEKERJAAN UMUM'}) set n:Main_Activity;
match (n:Activity{name:'PEKERJAAN ARSITEKTUR'}) set n:Main_Activity;
match (n:Activity{name:'PEKERJAAN MEP'}) set n:Main_Activity;
match (n:Activity{name:'PEKERJAAN INTERIOR'}) set n:Main_Activity;



match (n:Activity) 
where n.index_wbs contains '.' 
with n
, size(n.index_wbs) as sis
where sis=3
match (m:Main_Activity{index_wbs:left(n.index_wbs,1)})
MERGE (m)-[r:SUB_ACTIVITY]->(n)
set n:Activity_2
;

match (n:Activity) 
where n.index_wbs contains '.' 
with n
, size(n.index_wbs) as sis
where sis=5
match (m:Activity_2{index_wbs:left(n.index_wbs,3)})
MERGE (m)-[r:SUB_ACTIVITY]->(n)
set n:Activity_3
;

match (i:Item)
where i.text<>''
    detach delete i;



