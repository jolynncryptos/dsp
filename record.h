#ifndef RECORD_H
#define RECORD_H

#include <string>
#include <vector>

struct Record {
    std::string GAME_DATE_EST;
    int TEAM_ID_home;
    int PTS_home;
    double FG_PCT_home;
    double FT_PCT_home;
    double FG3_PCT_home;
    int AST_home;
    int REB_home;
    int HOME_TEAM_WINS;

    static Record fromCSV(const std::string &line);
    size_t size() const;  // Return approximate size in bytes
};

#endif

