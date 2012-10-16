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
          case ttEOF:
            return "end of input";
          default:
            return "unknown token";
        }
      }
    }
    public final Type _type;
    public final double _value;
    public final int _valueInt;
    public final String _id;
    public final int _pos;
    
    public Token(int pos, Type type) {
      _pos = pos;
      this._type = type;
      _value = 0;
      _valueInt = 0;
      _id = "";
    }
    
    public Token(int pos, double d) {
      _pos = pos;
      this._type = Type.ttFloat;
      _value = d;
      _valueInt = (int) d;
      _id = "";
    }

    public Token(int pos, int  i) {
      _pos = pos;
      this._type = Type.ttInteger;
      _value = i;
      _valueInt = i;
      _id = "";
    }
    
    public Token(int pos, String s) {
      _pos = pos;
      this._type = Type.ttIdent;
      _value = 0;
      _valueInt = 0;
      _id = s;
    }
  }
 
  private Stream _s;
  
  private Token _top;
  
  protected Token top() {
    return _top;  
  }
  
  protected Token pop() throws ParserException {
    Token x = _top;
    _top = parseNextToken();
    return x;
  }

  protected Token pop(Token.Type type) throws ParserException {
    if (top()._type != type)
      throw new ParserException(top()._pos, type,top()._type);
    return pop();
  }
  
  private void skipWhitespace() {
    char c;
    while (true) {
      if (_s.eof())
        break;
      c = (char) _s.peek1();
      if ((c == ' ') || (c == '\t') || (c == '\n')) {
        _s.get1();
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
    int pos = _s._off;
    if (_s.eof())
      return new Token(pos, Token.Type.ttEOF);
    char c = (char) _s.peek1();
    switch (c) {
      case '=':
        ++_s._off;
        return new Token(pos, Token.Type.ttOpAssign);
      case '$':
        ++_s._off;
        return new Token(pos, Token.Type.ttOpDollar);
      case '+':
        ++_s._off;
        return new Token(pos, Token.Type.ttOpAdd);
      case '-':
        ++_s._off;
        if (_s.peek1() == '>') {
          ++_s._off;
          return new Token(pos,Token.Type.ttOpRightAssign);
        } else {
          return new Token(pos, Token.Type.ttOpSub);
        }
      case '*':
        ++_s._off;
        return new Token(pos, Token.Type.ttOpMul);
      case '/':
        ++_s._off;
        return new Token(pos, Token.Type.ttOpDiv);
      case '(':
        ++_s._off;
        return new Token(pos, Token.Type.ttOpParOpen);
      case ')':
        ++_s._off;
        return new Token(pos, Token.Type.ttOpParClose);
      case '[':
        ++_s._off;
        return new Token(pos, Token.Type.ttOpBracketOpen);
      case ']':
        ++_s._off;
        return new Token(pos, Token.Type.ttOpBracketClose);
      case '<':
        ++_s._off;
        if (_s.peek1()=='-') {
          ++_s._off;
          return new Token(pos,Token.Type.ttOpAssign);
        } else {
          return new Token(pos,Token.Type.ttOpLess);
        }
      case '>':
        ++_s._off;
        return new Token(pos, Token.Type.ttOpGreater);
      case '"':
        return parseIdent();
      default:
        if (isCharacter(c)) 
          return parseIdent();
        if (isDigit(c))
          return parseNumber();
    }
    return new Token(pos, Token.Type.ttUnknown);
  }

  private Token parseIdent() throws ParserException {
    int start = _s._off;
    if (_s.peek1() == '"') { // escaped string
      ++_s._off;
      StringBuilder sb = new StringBuilder();
      while (true) {
        if (_s.eof())
          throw new ParserException(start,"String does not finish before the end of input");
        if (_s.peek1() == '"') {
          ++_s._off;
          break; // end of the string
        } else if (_s.peek1()=='\\') {
          ++_s._off;
          if (_s.eof())
            throw new ParserException(start,"String does not finish before the end of input");
          switch (_s.peek1()) {
            case '"':
            case '\\':
            case '\'':
              break;
            default:
              throw new ParserException(start,"Only quotes and backslash can be escaped in strings.");
          }
        }
        sb.append((char)_s.get1());
      }
      return new Token(start,sb.toString());
    } else {
      while (true) {
        if (_s.eof())
          break;
        char c = (char) _s.peek1();
        if (isCharacter(c) || isDigit(c) || (c=='.')) {
          ++_s._off;
          continue;
        }
        break;
      }
    }
    return new Token(start,new String(_s._buf, start, _s._off - start));
  }
  
  private Token parseNumber() throws ParserException {
    int start = _s._off;
    boolean dot = false;
    boolean e = false;
    while (true) {
      if (_s.eof())
        break;
      char c = (char) _s.peek1();
      if (isDigit(c)) {
        ++_s._off;
        continue;
      }
      if (c == '.') {
        if (dot != false)
          throw new ParserException(_s._off,"Only one dot can be present in number.");
        dot = true;
        ++_s._off;
        continue;
      }
      if ((c == 'e') || (c == 'E')) {
        if (e != false)
          throw new ParserException(_s._off,"Only one exponent can be present in number.");
        e = true;
        ++_s._off;
        continue;
      }
      break;
    }
    if ((dot == false) && (e == false)) 
      return new Token(start, Integer.parseInt(new String(_s._buf, start, _s._off - start)));
    else
      return new Token(start, Double.parseDouble(new String(_s._buf, start, _s._off - start)));
  }
  
  // ---------------------------------------------------------------------------
  // Parser part
  //
  // A simple LL(1) recursive descent guy. With the following grammar:

  public Expr parse(String x) throws ParserException {
    return parse(new Stream(x.getBytes()));
  }
  
  public Expr parse(Stream x) throws ParserException {
    _s = x;
    pop(); // load the first token in the stream
    Expr result = parse_S();
    pop(Token.Type.ttEOF); // make sure we have parsed everything
    return result;
  }
  
  
  /** Parses the expression in R. 
   * 
   * S -> e | E [ -> ident ]
   *
   */
  private Expr parse_S() throws ParserException {
    if (top()._type == Token.Type.ttEOF)
      return null;
    Expr result = parse_E();
    if (top()._type == Token.Type.ttOpRightAssign) {
      int pos = pop()._pos;
      result = new AssignmentOperator(pos, Key.make(pop()._id), result);
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
    while ((top()._type == Token.Type.ttOpAdd) || (top()._type == Token.Type.ttOpSub)) {
      Token t = pop();
      result = new BinaryOperator(t._pos,t._type,result,parse_T());
    }
    return result;
  }
  
  /*
   * T -> F { * F | / F }
   */
  private Expr parse_T() throws ParserException {
    Expr result = parse_F();
    while ((top()._type == Token.Type.ttOpMul) || (top()._type == Token.Type.ttOpDiv)) {
      Token t = pop();
      result = new BinaryOperator(t._pos,t._type,result,parse_T());
    }
    return result;
  }
  
  /*
   * This is silly grammar for now, I need to understand R more to make it 
   * 
   * F -> - F | number | ident (  = S | $ ident | [ number ] ) | ( S ) 
   */
  
  private Expr parse_F() throws ParserException {
    int pos = top()._pos;
    switch (top()._type) {
      case ttOpSub:
        return new UnaryOperator(pos,pop()._type,parse_F());
      case ttFloat:
        return new FloatLiteral(pos, pop()._value);
      case ttInteger:
        return new FloatLiteral(pos, pop()._value);
      case ttIdent: {
        Token t = pop();
        if (top()._type == Token.Type.ttOpAssign) {
          pos = pop()._pos;
          Expr rhs = parse_S();
          return new AssignmentOperator(pos, Key.make(t._id), rhs);
        } else if (top()._type == Token.Type.ttOpDollar) {
          pos = pop()._pos;
          return new StringColumnSelector(pos,new KeyLiteral(t._pos,t._id), pop(Token.Type.ttIdent)._id);
        } else if (top()._type == Token.Type.ttOpBracketOpen) {
          pos = pop()._pos;
          int idx = pop(Token.Type.ttInteger)._valueInt;
          pop(Token.Type.ttOpBracketClose);
          return new ColumnSelector(pos,new KeyLiteral(t._pos,t._id), idx);
        } else {  
          return new KeyLiteral(t._pos,t._id);
        }
      }
      case ttOpParOpen: {
        pop();
        Expr e = parse_S();
        pop(Token.Type.ttOpParClose);
        return e;
      }
      default:
        throw new ParserException(top()._pos,"Number or parenthesis",top()._type);
    }
  }
  
}
