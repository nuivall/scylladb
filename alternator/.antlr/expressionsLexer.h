
	#include "alternator/expressions.hh"
	// ANTLR generates a bunch of unused variables and functions. Yuck...
    #pragma GCC diagnostic ignored "-Wunused-variable"
    #pragma GCC diagnostic ignored "-Wunused-function"


// Generated from /code/nuivall/scylladb/alternator/expressions.g by ANTLR 4.9.2

#pragma once


#include "antlr4-runtime.h"




    void displayRecognitionError(ANTLR_UINT8** token_names, ExceptionBaseType* ex) {
        throw expressions_syntax_error("syntax error");
    }


class  expressionsLexer : public antlr4::Lexer {
public:
  enum {
    T__0 = 1, T__1 = 2, WHITESPACE = 3, SET = 4, REMOVE = 5, ADD = 6, DELETE = 7, 
    AND = 8, OR = 9, NOT = 10, BETWEEN = 11, IN = 12, INTEGER = 13, NAME = 14, 
    NAMEREF = 15, VALREF = 16, OPEN_BRACKET = 17, CLOSE_BRACKET = 18, OPEN_SQUARE_BRACKET = 19, 
    CLOSE_SQUARE_BRACKET = 20, COMMA = 21, EQ = 22, DOT = 23, PLUS = 24, 
    MINUS = 25
  };

  explicit expressionsLexer(antlr4::CharStream *input);
  ~expressionsLexer();

  virtual std::string getGrammarFileName() const override;
  virtual const std::vector<std::string>& getRuleNames() const override;

  virtual const std::vector<std::string>& getChannelNames() const override;
  virtual const std::vector<std::string>& getModeNames() const override;
  virtual const std::vector<std::string>& getTokenNames() const override; // deprecated, use vocabulary instead
  virtual antlr4::dfa::Vocabulary& getVocabulary() const override;

  virtual const std::vector<uint16_t> getSerializedATN() const override;
  virtual const antlr4::atn::ATN& getATN() const override;

  virtual void action(antlr4::RuleContext *context, size_t ruleIndex, size_t actionIndex) override;
private:
  static std::vector<antlr4::dfa::DFA> _decisionToDFA;
  static antlr4::atn::PredictionContextCache _sharedContextCache;
  static std::vector<std::string> _ruleNames;
  static std::vector<std::string> _tokenNames;
  static std::vector<std::string> _channelNames;
  static std::vector<std::string> _modeNames;

  static std::vector<std::string> _literalNames;
  static std::vector<std::string> _symbolicNames;
  static antlr4::dfa::Vocabulary _vocabulary;
  static antlr4::atn::ATN _atn;
  static std::vector<uint16_t> _serializedATN;


  // Individual action functions triggered by action() above.
  void WHITESPACEAction(antlr4::RuleContext *context, size_t actionIndex);

  // Individual semantic predicate functions triggered by sempred() above.

  struct Initializer {
    Initializer();
  };
  static Initializer _init;
};

