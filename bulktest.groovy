import groovy.sql.Sql
import java.sql.SQLException

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
       doInsert(sql, ((i * batchpercommit) + j) * batchsize, batchsize, false)
     }
     doCommit(sql);
   }
} catch( SQLException sqle ){
    sqle.printStackTrace()
} finally {
    sql?.close()
}

def doInsert(sql, startindex, numrecs, makeError) {
//  println "starting batch insert from index: " + startindex + ", batchsize: " + numrecs

  if (numrecs <= 1) {
    def eno = startindex
    sql.execute("insert into emp1(empno, ename, job, sal, deptno) values (" + eno + ", 'name " + eno + "', '" + textData + "', " + (100 + eno) + ", 0)")
  } else {
    sql.withBatch(numrecs, "insert into emp1(empno, ename, job, sal, deptno) values (?, ?, ?, ?, ?)", { ps -> 
	for (i = 0;i < numrecs;++i) {
		def eno = i + startindex
		ps.addBatch(eno, "name " + eno, textData, 100 + eno, 0)
	}
    })
  }
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
        DEPTNO NUMBER(2))
''')

}

