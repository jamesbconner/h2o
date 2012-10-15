package water.exec;

import water.Key;
import water.Stream;

/**
 *
 * @author peta
 */
public class RLikeParser {
  
  // ---------------------------------------------------------------------------
  // Lexer part 
  //
  // A simple lexer that can support a LL(1) grammar for the time being.
  // We might get more complicated here in the future. But for now, I can't care
  // less.
  // ---------------------------------------------------------------------------
  
  public static class Token {
    public enum Type {
      ttFloat, // any floating point number
      ttInteger, // integer number
      ttIdent, // any identifier
      ttOpAssign, // assignment, = or <-
      ttOpRightAssign, // -> assignment in R
      ttOpDollar, // $
      ttOpAdd, // +
      ttOpSub, // -
      ttOpMul, // *
      ttOpDiv, // /
      ttOpParOpen, // (
      ttOpParClose, // )
      ttOpBracketOpen, // [
      ttOpBracketClose, // ]
      ttOpDoubleQuote, // "
      ttOpLess, // <
      ttOpGreater, // >
      ttEOF,
      ttUnknown,
      ;
      public String toString() {
        switch (this) {
          case ttFloat:
            return "float";
          case ttInteger:
            return "integer";
          case ttIdent:
            return "identifier";
          case ttOpAssign:
            return "assignment";
          case ttOpRightAssign:
            return "assignment to the right";
          case ttOpDollar:
            return "membership $";
          case ttOpAdd:
            return "operator +";
          case ttOpSub:
            return "operator -";
          case ttOpMul:
            return "operator *";
          case ttOpDiv:
            return "operator /";
          case ttOpLess:
            return "operator <";
          case ttOpGreater:
            return "operator >";
          case ttOpParOpen:
            return "opening parenthesis";
          case ttOpParClose:
            return "closing parenthesis";
          case ttOpBracketOpen:
            return "opening bracket";
          case ttOpBracketClose:
            return "closing bracket";
          case ttOpDoubleQuote:
            return "\"";
          case ttEOF:
            return "end of input";
          default:
            return "unknown token";
        }
      }
    }
    public final Type type;
    public final double value;
    public final int valueInt;
    public final String id;
    public final int _pos;
    
    public Token(int pos, Type type) {
      _pos = pos;
      this.type = type;
      value = 0;
      valueInt = 0;
      id = "";
    }
    
    public Token(int pos, double d) {
      _pos = pos;
      this.type = Type.ttFloat;
      value = d;
      valueInt = (int) d;
      id = "";
    }

    public Token(int pos, int  i) {
      _pos = pos;
      this.type = Type.ttInteger;
      value = i;
      valueInt = i;
      id = "";
    }
    
    public Token(int pos, String s) {
      _pos = pos;
      this.type = Type.ttIdent;
      value = 0;
      valueInt = 0;
      id = s;
    }
  }
 
  private Stream s_;
  
  private Token top_;
  
  protected Token top() {
    return top_;  
  }
  
  protected Token pop() throws ParserException {
    Token x = top_;
    top_ = parseNextToken();
    return x;
  }

  protected Token pop(Token.Type type) throws ParserException {
    if (top().type != type)
      throw new ParserException(top()._pos, type,top().type);
    return pop();
  }
  
  private void skipWhitespace() {
    char c;
    while (true) {
      if (s_.eof())
        break;
      c = (char) s_.peek1();
      if ((c == ' ') || (c == '\t') || (c == '\n')) {
        s_.get1();
        continue;
      }
      break;
    }
  }
  
  private boolean isCharacter(char c) {
    if ((c >= 'a') && (c <= 'z'))
      return true;
    if ((c >= 'A') && (c <= 'Z'))
      return true;
    return c == '_';
  }
  
  private boolean isDigit(char c) {
    return (c>='0') && (c <='9');
  }
  
  private Token parseNextToken() throws ParserException {
    skipWhitespace();
    int pos = s_._off;
    if (s_.eof())
      return new Token(pos, Token.Type.ttEOF);
    char c = (char) s_.peek1();
    switch (c) {
      case '=':
        ++s_._off;
        return new Token(pos, Token.Type.ttOpAssign);
      case '$':
        ++s_._off;
        return new Token(pos, Token.Type.ttOpDollar);
      case '+':
        ++s_._off;
        return new Token(pos, Token.Type.ttOpAdd);
      case '-':
        ++s_._off;
        if (s_.peek1() == '>') {
          ++s_._off;
          return new Token(pos,Token.Type.ttOpRightAssign);
        } else {
          return new Token(pos, Token.Type.ttOpSub);
        }
      case '*':
        ++s_._off;
        return new Token(pos, Token.Type.ttOpMul);
      case '/':
        ++s_._off;
        return new Token(pos, Token.Type.ttOpDiv);
      case '(':
        ++s_._off;
        return new Token(pos, Token.Type.ttOpParOpen);
      case ')':
        ++s_._off;
        return new Token(pos, Token.Type.ttOpParClose);
      case '[':
        ++s_._off;
        return new Token(pos, Token.Type.ttOpBracketOpen);
      case ']':
        ++s_._off;
        return new Token(pos, Token.Type.ttOpBracketClose);
      case '"':
        ++s_._off;
        return new Token(pos, Token.Type.ttOpDoubleQuote);
      case '<':
        ++s_._off;
        if (s_.peek1()=='-') {
          ++s_._off;
          return new Token(pos,Token.Type.ttOpAssign);
        } else {
          return new Token(pos,Token.Type.ttOpLess);
        }
      case '>':
        ++s_._off;
        return new Token(pos, Token.Type.ttOpGreater);
      default:
        if (isCharacter(c)) 
          return parseIdent();
        if (isDigit(c))
          return parseNumber();
    }
    return new Token(pos, Token.Type.ttUnknown);
  }

  private Token parseIdent() {
    int start = s_._off;
    while (true) {
      if (s_.eof())
        break;
      char c = (char) s_.peek1();
      if (isCharacter(c) || isDigit(c) || (c=='.')) {
        ++s_._off;
        continue;
      }
      break;
    }
    return new Token(start,new String(s_._buf, start, s_._off - start));
  }
  
  private Token parseNumber() throws ParserException {
    int start = s_._off;
    boolean dot = false;
    boolean e = false;
    while (true) {
      if (s_.eof())
        break;
      char c = (char) s_.peek1();
      if (isDigit(c)) {
        ++s_._off;
        continue;
      }
      if (c == '.') {
        if (dot != false)
          throw new ParserException(s_._off,"Only one dot can be present in number.");
        dot = true;
        ++s_._off;
        continue;
      }
      if ((c == 'e') || (c == 'E')) {
        if (e != false)
          throw new ParserException(s_._off,"Only one exponent can be present in number.");
        e = true;
        ++s_._off;
        continue;
      }
      break;
    }
    if ((dot == false) && (e == false)) 
      return new Token(start, Integer.parseInt(new String(s_._buf, start, s_._off - start)));
    else
      return new Token(start, Double.parseDouble(new String(s_._buf, start, s_._off - start)));
  }
  
  // ---------------------------------------------------------------------------
  // Parser part
  //
  // A simple LL(1) recursive descent guy. With the following grammar:

  public Expr parse(String x) throws ParserException {
    return parse(new Stream(x.getBytes()));
  }
  
  public Expr parse(Stream x) throws ParserException {
    s_ = x;
    pop(); // load the first token in the stream
    if (top().type == Token.Type.ttOpDoubleQuote) {
      pop(); 
      Expr result = parse_S();
      pop(Token.Type.ttOpDoubleQuote);
      pop(Token.Type.ttEOF); // make sure we have parsed everything
      return result;
    } else {
      Expr result = parse_S();
      pop(Token.Type.ttEOF); // make sure we have parsed everything
      return result;
    }
  }
  
  
  /** Parses the expression in R. 
   * 
   * S -> e | E [ -> ident ]
   *
   */
  private Expr parse_S() throws ParserException {
    if (top().type == Token.Type.ttEOF)
      return null;
    Expr result = parse_E();
    if (top().type == Token.Type.ttOpRightAssign) {
      int pos = pop()._pos;
      result = new AssignmentOperator(pos, Key.make(pop().id), result);
    }
    return result;
  }
  
  
  /* 
   * 
   * 
   * E -> T { + T | - T }
   * 
   * @return 
   */
  private Expr parse_E() throws ParserException {
    Expr result = parse_T();
    while ((top().type == Token.Type.ttOpAdd) || (top().type == Token.Type.ttOpSub)) {
      Token t = pop();
      result = new BinaryOperator(t._pos,t.type,result,parse_T());
    }
    return result;
  }
  
  /*
   * T -> F { * F | / F }
   */
  private Expr parse_T() throws ParserException {
    Expr result = parse_F();
    while ((top().type == Token.Type.ttOpMul) || (top().type == Token.Type.ttOpDiv)) {
      Token t = pop();
      result = new BinaryOperator(t._pos,t.type,result,parse_T());
    }
    return result;
  }
  
  /*
   * This is silly grammar for now, I need to understand R more to make it 
   * 
   * F -> number | ident (  = S | $ ident | [ number ] ) | ( S ) 
   */
  
  private Expr parse_F() throws ParserException {
    int pos = top()._pos;
    switch (top().type) {
      case ttFloat:
        return new FloatLiteral(pos, pop().value);
      case ttInteger:
        return new FloatLiteral(pos, pop().value);
      case ttIdent: {
        Token t = pop();
        if (top().type == Token.Type.ttOpAssign) {
          pos = pop()._pos;
          Expr rhs = parse_S();
          return new AssignmentOperator(pos, Key.make(t.id), rhs);
        } else if (top().type == Token.Type.ttOpDollar) {
          pos = pop()._pos;
          return new StringColumnSelector(pos,new KeyLiteral(t._pos,t.id), pop(Token.Type.ttIdent).id);
        } else if (top().type == Token.Type.ttOpBracketOpen) {
          pos = pop()._pos;
          int idx = pop(Token.Type.ttInteger).valueInt;
          pop(Token.Type.ttOpBracketClose);
          return new ColumnSelector(pos,new KeyLiteral(t._pos,t.id), idx);
        } else {  
          return new KeyLiteral(t._pos,t.id);
        }
      }
      case ttOpParOpen: {
        pop();
        Expr e = parse_S();
        pop(Token.Type.ttOpParClose);
        return e;
      }
      default:
        throw new ParserException(top()._pos,"Number or parenthesis",top().type);
    }
  }
  
}
