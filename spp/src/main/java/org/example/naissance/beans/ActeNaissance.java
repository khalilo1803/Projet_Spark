package org.example.naissance.beans;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;

import java.io.Serializable;
@Data
@Getter
@Builder
public class ActeNaissance implements Serializable {

    private String COD_VAR ;
    private String  LIB_VAR ;
    private String  COD_MOD;
    private String LIB_MOD;
    private String TYPE_VAR;
    private String LONG_VAR;
}
