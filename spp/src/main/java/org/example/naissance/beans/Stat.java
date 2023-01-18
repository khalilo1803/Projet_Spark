package org.example.naissance.beans;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;
@RequiredArgsConstructor
@Data
@Builder
@AllArgsConstructor
public class Stat implements Serializable {

    private String MOD ="" ;
    private int  COUNT =1  ;

}
