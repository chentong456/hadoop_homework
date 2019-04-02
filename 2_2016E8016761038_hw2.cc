/*
* wo yi ding shi nao zi bei lv ti le ,cai xuan de zhe ge ke a !!!:wq
nao zi you bing a. wo yao hui jia zhong tian
 nao zi you bing a.
 maya wo xiang hui jia zhong tian le
 */

#include <stdio.h>
#include <string.h>
#include <math.h>
#include <set>
#include "GraphLite.h"
#include <iostream>
#define VERTEX_CLASS_NAME(name) GraphColor##name
using namespace std; 
#define EPS 1e-6

int64_t start;// the input vertex id
int colNum;//the input vertex color number

class VERTEX_CLASS_NAME(InputFormatter): public InputFormatter {
public:
    int64_t getVertexNum() {
        unsigned long long n;
        sscanf(m_ptotal_vertex_line, "%lld", &n);
        m_total_vertex= n;
        return m_total_vertex;
    }
    int64_t getEdgeNum() {
        unsigned long long n;
        sscanf(m_ptotal_edge_line, "%lld", &n);
        m_total_edge= n;
        return m_total_edge;
    }
    int getVertexValueSize() {
        m_n_value_size = sizeof(int);
        return m_n_value_size;
    }
    int getEdgeValueSize() {
        m_e_value_size = sizeof(double);
        return m_e_value_size;
    }
    int getMessageValueSize() {
        m_m_value_size = sizeof(int);
        return m_m_value_size;
    }
    void loadGraph() {
        unsigned long long last_vertex;
        unsigned long long from;
        unsigned long long to;
		unsigned long long h;
        double weight = 0;
        
        //double value = 1;
		int value=-1;//initialize all color=-1
        int outdegree = 0;
        
        const char *line= getEdgeLine();

        // Note: modify this if an edge weight is to be read
        //       modify the 'weight' variable

        sscanf(line, "%lld %lld %lld", &from, &to,&h);
        addEdge(from, to, &weight);

        last_vertex = from;
        ++outdegree;
        for (int64_t i = 1; i < m_total_edge; ++i) {
            line= getEdgeLine();

            // Note: modify this if an edge weight is to be read
            //       modify the 'weight' variable

            sscanf(line, "%lld %lld", &from, &to);
            if (last_vertex != from) {
                addVertex(last_vertex, &value, outdegree);
                last_vertex = from;
                outdegree = 1;
            } else {
                ++outdegree;
            }
            addEdge(from, to, &weight);
        }
        addVertex(last_vertex, &value, outdegree);
    }
};

class VERTEX_CLASS_NAME(OutputFormatter): public OutputFormatter {
public:
    void writeResult() {
        int64_t vid;
        //double value;
		int value;
        char s[1024];

        for (ResultIterator r_iter; ! r_iter.done(); r_iter.next() ) {
            r_iter.getIdValue(vid, &value);
            int n = sprintf(s, "%lld: %d\n", (unsigned long long)vid, value);
            writeNextResLine(s, n);
        }
    }
};

// An aggregator that records a double value to compute sum
class VERTEX_CLASS_NAME(Aggregator): public Aggregator<int> {
public:
    void init() {
        m_global = 0;
        m_local = 0;
    }
    void* getGlobal() {
        return &m_global;
    }
    void setGlobal(const void* p) {
        m_global = * (double *)p;
    }
    void* getLocal() {
        return &m_local;
    }
	
    void merge(const void* p) {
        m_global += * (double *)p;
    }
    void accumulate(const void* p) {
        m_local += * (double *)p;
    }
};

class VERTEX_CLASS_NAME(): public Vertex <int, double, int> {
public:
    void compute(MessageIterator* pmsgs) {
		int val;
        if (getSuperstep() == 0) {
		    val=-1;//all vertexes color are -1
			if (getVertexId()==start){//the command line argv[3] required
				val=0;
			}
		}
		else { 	
			//find a num which is different from all the neighbour vertexes
	    	//cout<<"hhhhhhhhhhhhhhhhhhhhhh"<<endl;
			set<int> s;
			for (; ! pmsgs->done(); pmsgs->next()) {
				//cout<<(int)(pmsgs->getValue())<<endl;
				s.insert((int)pmsgs->getValue());
			}
			val=getValue();

			if(val!=-1){
				set<int>::iterator it1;
				it1=s.find(val);
				if(it1==s.end()){
					voteToHalt();
					return;
				}
			}
			  val=rand()%colNum;
			  set<int>::iterator it;
			  for(it=s.begin();it!=s.end();it++){
			  	if(val==*it){
					val=rand()%colNum;
					it=s.begin();
				}
			  }
			  //double acc=fabs(getValue()-val);
			  //accumulateAggr(0,&acc);
			//cout<<"hhhhhhhhhhhhhhhhh"<<val<<endl;			  
		}
        * mutableValue() = val;//mutate/change the vertex value.
		sendMessageToAllNeighbors(val);
    }
};

class VERTEX_CLASS_NAME(Graph): public Graph {
public:
    VERTEX_CLASS_NAME(Aggregator)* aggregator;

public:
    // argv[0]: PageRankVertex.so
    // argv[1]: <input path>
    // argv[2]: <output path>
	// argv[3]: <the start vertex V0>
	// argv[4]: <the total number of color>
    void init(int argc, char* argv[]) {

        setNumHosts(5);
        setHost(0, "localhost", 1411);
        setHost(1, "localhost", 1421);
        setHost(2, "localhost", 1431);
        setHost(3, "localhost", 1441);
        setHost(4, "localhost", 1451);

        if (argc < 5) {
           printf ("Usage: %s <input path> <output path>\n", argv[0]);
           exit(1);
        }

        m_pin_path = argv[1];
        m_pout_path = argv[2];
		//start =_atoi64(argv[3]);
		int aa=atoi(argv[3]);
		start=(int64_t)aa;
		//cout<<"hhhhhhhhhhhhhhhhhhh";
		//cout<<aa<<endl;
		//start=int64_t S64(*argv[3])
		colNum =atoi(argv[4]);
        aggregator = new VERTEX_CLASS_NAME(Aggregator)[1];//allocate memeory for the Aggregator
        regNumAggr(1);//register the number of aggregators
        regAggr(0, &aggregator[0]);//register Aggregator ID 0
    }

    void term() {
        delete[] aggregator;
    }
};

/* STOP: do not change the code below. */
extern "C" Graph* create_graph() {
    Graph* pgraph = new VERTEX_CLASS_NAME(Graph);

    pgraph->m_pin_formatter = new VERTEX_CLASS_NAME(InputFormatter);
    pgraph->m_pout_formatter = new VERTEX_CLASS_NAME(OutputFormatter);
    pgraph->m_pver_base = new VERTEX_CLASS_NAME();

    return pgraph;
}

extern "C" void destroy_graph(Graph* pobject) {
    delete ( VERTEX_CLASS_NAME()* )(pobject->m_pver_base);
    delete ( VERTEX_CLASS_NAME(OutputFormatter)* )(pobject->m_pout_formatter);
    delete ( VERTEX_CLASS_NAME(InputFormatter)* )(pobject->m_pin_formatter);
    delete ( VERTEX_CLASS_NAME(Graph)* )pobject;
}
