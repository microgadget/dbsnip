import groovy.sql.Sql
import java.sql.SQLException
import oracle.sql.*
import oracle.jdbc.internal.*

textData = """
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
"""
print textData.length()

try {
    sql = Sql.newInstance(
        "jdbc:oracle:thin:@192.168.1.133:1521:exampledb"
        ,"scott", "tiger"
        ,"oracle.jdbc.driver.OracleDriver")

   doPrepare(sql)
   // batchsize は bulk insertする行数、1 にすると単純なinsert分を発行
   def batchsize = 10
   // batchpercommit は、コミットするまで何回bulk insertするかの数
   def batchpercommit = 4
   // commitcount は コミットする回数。
   def commitcount = 1000
   // 全行数は、 batchsize * batchpercommit * commitcount になる。

   for (int i = 0;i < commitcount;++i) {
     for (int j = 0;j < batchpercommit;++j) {
   	prepareDesc(sql)
       doInsert(sql, ((i * batchpercommit) + j) * batchsize, batchsize)
     }
     doCommit(sql);
   }

} catch( SQLException sqle ){
    sqle.printStackTrace()
} finally {
    sql?.close()
}


def enosDesc
def enamesDesc
def jobsDesc
def salsDesc
def deptnosDesc
def prepareDesc(sql) {
	def conn = sql.getConnection()
	enosDesc = ArrayDescriptor.createDescriptor("EMPNOARRAY", conn)
	enamesDesc = ArrayDescriptor.createDescriptor("ENAMEARRAY", conn)
	jobsDesc = ArrayDescriptor.createDescriptor("JOBARRAY", conn)
	salsDesc = ArrayDescriptor.createDescriptor("SALARRAY", conn);
	deptnosDesc = ArrayDescriptor.createDescriptor("DEPTNOARRAY", conn)
}

def doInsert(sql, startindex, numrecs) {
//  println "starting batch insert from index: " + startindex + ", batchsize: " + numrecs
    def enos = []
    def enames = []
    def jobs = []
    def sals = []
    def deptnos = []

    for (int i = 0;i < numrecs;++i) {
        def eno = startindex + i
    	enos.add(new Integer(eno))
    	enames.add("name " + eno)
    	jobs.add(textData)
    	sals.add(new Integer(1000 + eno % 10))
    	deptnos.add(new Integer(eno % 5))
    }
    def conn = sql.getConnection();
    def arrEnos = new ARRAY(enosDesc, conn, enos.toArray() )
    def arrEnames = new ARRAY(enamesDesc, conn, enames.toArray() )
    def arrJobs = new ARRAY(jobsDesc, conn, jobs.toArray() )
    def arrSals = new ARRAY(salsDesc, conn, sals.toArray() )
    def arrDeptnos = new ARRAY(deptnosDesc, conn, deptnos.toArray() )
    sql.call("{call INSERTEMP(?, ?, ?, ?, ?)}", [arrEnos, arrEnames, arrJobs, arrSals, arrDeptnos])
}

def doCommit(sql) {
	sql.commit()
}

def doPrepare(sql) {
sql.execute('''
drop table EMP1
''')
sql.execute('''
create table EMP1
   (EMPNO NUMBER(8) NOT NULL,
        ENAME VARCHAR2(100),
        JOB VARCHAR2(2000),
        MGR NUMBER(8),
        HIREDATE DATE,
        SAL NUMBER(10,2),
        COMM NUMBER(10,2),
        DEPTNO NUMBER(4))
''')
sql.execute("create or replace type EmpNoArray as varray(20) of NUMBER(8)")
sql.execute("create or replace type ENameArray as varray(20) of VARCHAR2(100)")
sql.execute("create or replace type JobArray as varray(20) of VARCHAR2(2000)")
sql.execute("create or replace type MgrArray as varray(20) of NUMBER(8)")
sql.execute("create or replace type HireDateArray as varray(20) of DATE")
sql.execute("create or replace type SalArray as varray(20) of NUMBER(10,2)")
sql.execute("create or replace type CommArray as varray(20) of NUMBER(10,2)")
sql.execute("create or replace type DeptNoArray as varray(20) of NUMBER(4)")
sql.execute('''
create or replace procedure InsertEmp(
enos in EMPNOARRAY,
enames in ENAMEARRAY,
jobs in JOBARRAY,
sals in SALARRAY,
deptnos in DEPTNOARRAY
)
as
  i integer;
begin
  for i in 1..enos.count loop
    insert into emp1(empno, ename, job, sal, deptno) values(enos(i), enames(i), jobs(i), sals(i), deptnos(i));
  end loop;
end;
''')

}


