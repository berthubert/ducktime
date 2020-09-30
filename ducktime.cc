#include "duckdb.hpp"
#include "duckdb/main/appender.hpp"
#include <string>
#include <iostream>
#include <map>
#include <variant>
#include <sys/time.h>
using namespace std;


/** Class which you can give a DuckDB name and you can then 
    stream data to it. Will create fields as needed */
class DuckTime
{
public:
  explicit DuckTime(string_view fname) : d_db(&fname[0]), d_con(d_db)
  {
    d_con.BeginTransaction();
  }

  ~DuckTime()
  {
    cerr<<"Closing appender"<<endl;
    if(d_appender)
      d_appender->Close();
    cerr<<"Committing transaction"<<endl;
    d_con.Commit();
    cerr<<"Done with commit"<<endl;
  }

  void cycle()
  {
    if(d_appender)
      d_appender->Flush();
  }
  typedef std::variant<double, int32_t, uint32_t, int64_t, string> var_t;
  //! store a datum. Tags could be indexed later.
  void addValue(const std::vector<std::pair<std::string, var_t>>& tags, std::string name, const initializer_list<std::pair<const char*, var_t>>& values, double t);
  
private:
  // for each table, the known types
  std::map<std::string, std::vector<std::pair<std::string, std::string> > > d_types;
  duckdb::DuckDB d_db;
  duckdb::Connection d_con;
  std::unique_ptr<duckdb::Appender> d_appender;
};
using namespace duckdb;

//! Get field names and types from a table
vector<pair<string,string> > getSchema(Connection& con, string_view table)
{
  auto stmt = con.Prepare("SELECT column_name, data_type FROM information_schema_columns() WHERE table_schema='main' AND table_name=? ORDER BY ordinal_position");
  if(!stmt->success) {
    throw runtime_error("Unable to prepare query for schema retrieval: "+stmt->error);
  }
  cerr<<"Get schema prepare done, executing it for '"<<table<<"'"<<endl;
  auto res = stmt->Execute(&table[0]); // if you pass a string, it doesn't work?
  
  if(!res->success) {
    throw runtime_error("Unable to retrieve schema: "+stmt->error);
  }

  vector<pair<string,string>> ret;
  for(const auto& row : *res) {
    ret.push_back({row.GetValue<string>(0), row.GetValue<string>(1)});
  }
  return ret;
}


//! Add a column to a atable with a certain type
void addColumn(Connection& con, string_view table, string_view name, string_view type)
{
  // SECURITY PROBLEM - somehow we can't do prepared statements here
  auto stmt = con.Prepare("ALTER table "+string(table)+" add column \""+string(name)+ "\" "+string(type));

  // would love to do this, but we can't:
  //auto stmt = con.Prepare("ALTER table ? add column ? ?");
  if(!stmt->success) {
    throw std::runtime_error("Error preparing statement: "+stmt->error);
  }
  auto res = stmt->Execute();
  if(!res->success) {
    throw std::runtime_error("Error executing statement: "+res->error);
    return;
  }
}



void DuckTime::addValue(const std::vector<std::pair<std::string, var_t>>& tags, std::string name, const initializer_list<std::pair<const char*, var_t>>& values, double tstamp)
{
  auto& types = d_types[name];
  if(types.empty()) {
    cerr<<"Have no type information for table \""<<name<<"\""<<endl;
    types= getSchema(d_con, name);
    if(types.empty()) {
      auto res = d_con.Query("create table "+name+" (timestamp INT64)");
      if(!res->success) {
        throw std::runtime_error("Failed to create table '"+name+"': "+res->error);
      }
      
    }
  }
  bool addedSomething=false;
  // make sure all columns exist for the 'values'
  for(const auto& v : values) {
    if(auto iter = find_if(types.begin(), types.end(), [&v](const auto& a) { return a.first == v.first;} ); iter == types.end()) {
      cerr<<"Adding column "<<v.first<<endl;
      addColumn(d_con, name, v.first, "INT64");
      addedSomething=true;
    }
  }
  // these might also need indexes, the tags. We treat these as values for now
  for(const auto& v : tags) {
    if(auto iter = find_if(types.begin(), types.end(), [&v](const auto& a) { return a.first == v.first;} ); iter == types.end()) {
      cerr<<"Adding TAG column "<<v.first<<endl;
      if(std::get_if<double>(&v.second))
        addColumn(d_con, name, v.first, "DOUBLE");
      if(std::get_if<string>(&v.second))
        addColumn(d_con, name, v.first, "TEXT");
      else 
        addColumn(d_con, name, v.first, "INT64");
      addedSomething=true;
    }
  }

  
  if(addedSomething) {
    cerr<<"Rereading schema because we added something"<<endl;
    types = getSchema(d_con, name);
  }

  // ok, so now 'types' is up to date and in the right order

  // make sure we have an Appender
  if(!d_appender) {
    d_appender = make_unique<Appender>(d_con, name.c_str());
  }
  
  d_appender->BeginRow();
  // these are all the fields this table has, each of them needs to be in the Appender
  for(const auto& t : types) {
    bool appended=false;
    if(t.first == "timestamp") {
      d_appender->Append<int64_t>((int64_t)tstamp); 
      continue;
    }
    // consult the values
    for(const auto& v : values) {
      if(t.first == v.first) {
        std::visit([this](auto&& arg) {
            using T = std::decay_t<decltype(arg)>;
            //            cerr<<"   Appending "<<arg<<endl;
            d_appender->Append<T>(arg);
          }, v.second);
        appended=true;
        break;
      }
    }
    // consult the tags
    for(const auto& v : tags) {
      if(t.first == v.first) {
        std::visit([this](auto&& arg) {
            using T = std::decay_t<decltype(arg)>;
            //            cerr<<"   Appending TAG "<<arg<<endl;
            d_appender->Append<T>(arg);
          }, v.second);
        appended=true;
        break;
      }
    }

    
    if(!appended) {
      cerr<<"Did not get any data for column "<<t.first<<", inserting NULL"<<endl;
      d_appender->Append<nullptr_t>(nullptr);
    }
  }
  d_appender->EndRow();    

}


int main(int argc, char** argv)
try
{
  if(argc != 3) {
    cerr<<"Syntax: ducktime duckdbname number\nAdd 'number' items to the duckdbname database\n";
    return EXIT_FAILURE;
  }
  DuckTime dt(argv[1]);
  int limit = atoi(argv[2]);
  struct timeval tv;
  for(int n=0; n < limit; ++n) {
    double t;
    gettimeofday(&tv, 0);
    t= tv.tv_sec * 1000 + tv.tv_usec/1000.0;
    dt.addValue({{"server", n % 16}}, "network", {{"in", n*1234}, {"out", n*321}}, t);

    if(!(n% (1<<20))) {
      cerr<<"Cycle time: " << 100.0*n/limit << "%"<<endl;
      dt.cycle();
    }
  }
}
catch(std::exception& e)
{
  cerr<<"Fatal error: "<<e.what()<<endl;
}
